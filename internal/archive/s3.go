package archive

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/dokku/logpond/internal/segments"
)

// MultipartThreshold is the parquet size above which S3 uploads switch
// from PutObject to multipart upload (PRD §7.9.2).
const MultipartThreshold int64 = 64 << 20

// MultipartPartSize is the chunk size for multipart uploads. 16MB keeps
// the part count under 1024 even for >16GB segments while staying well
// above the 5MB minimum.
const MultipartPartSize int64 = 16 << 20

// MetadataParquetSHA is the S3 metadata key used for the idempotency
// check (PRD §7.9.2: "SHA-256 metadata header").
const MetadataParquetSHA = "parquet-sha256"

// S3API captures the subset of *s3.Client operations the backend uses.
// Tests inject a fake implementation; production wires the real client.
type S3API interface {
	HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	CreateMultipartUpload(ctx context.Context, params *s3.CreateMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error)
	UploadPart(ctx context.Context, params *s3.UploadPartInput, optFns ...func(*s3.Options)) (*s3.UploadPartOutput, error)
	CompleteMultipartUpload(ctx context.Context, params *s3.CompleteMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error)
	AbortMultipartUpload(ctx context.Context, params *s3.AbortMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error)
}

// S3Backend implements Backend against an S3-compatible store. The
// backend writes a parquet plus a sibling manifest object; the manifest
// is the commit marker.
type S3Backend struct {
	client S3API
	bucket string
	prefix string
	logger *slog.Logger

	// multipartThreshold lets tests force the multipart path without
	// requiring a 64MB fixture.
	multipartThreshold int64
	partSize           int64

	mu sync.Mutex
}

// S3Options configures S3Backend construction.
type S3Options struct {
	Client S3API
	Bucket string
	Prefix string
	Logger *slog.Logger

	MultipartThreshold int64
	PartSize           int64
}

// NewS3Backend constructs an S3Backend. The client is required; bucket
// must be non-empty.
func NewS3Backend(opts S3Options) (*S3Backend, error) {
	if opts.Client == nil {
		return nil, errors.New("s3 backend: Client is required")
	}
	if opts.Bucket == "" {
		return nil, errors.New("s3 backend: Bucket is required")
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	mt := opts.MultipartThreshold
	if mt <= 0 {
		mt = MultipartThreshold
	}
	ps := opts.PartSize
	if ps <= 0 {
		ps = MultipartPartSize
	}
	prefix := strings.Trim(opts.Prefix, "/")
	return &S3Backend{
		client:             opts.Client,
		bucket:             opts.Bucket,
		prefix:             prefix,
		logger:             opts.Logger,
		multipartThreshold: mt,
		partSize:           ps,
	}, nil
}

// Capabilities implements Backend. The S3 backend supports all modes.
func (b *S3Backend) Capabilities() Capabilities {
	return Capabilities{Archive: true, Retrieve: true, Verify: true, Name: "s3"}
}

// parquetKey returns the object key used for the parquet file. The
// layout matches PRD §7.9.2.
func (b *S3Backend) parquetKey(ref SegmentRef) string {
	return b.keyFor(ref, segments.ParquetFilename(ref.ID))
}

// manifestKey returns the sibling manifest object key.
func (b *S3Backend) manifestKey(ref SegmentRef) string {
	return b.keyFor(ref, segments.ManifestFilename(ref.ID))
}

func (b *S3Backend) keyFor(ref SegmentRef, filename string) string {
	t := ref.TimeStart.UTC()
	dir := fmt.Sprintf("segments/%04d/%02d/%02d/%02d", t.Year(), t.Month(), t.Day(), t.Hour())
	parts := []string{}
	if b.prefix != "" {
		parts = append(parts, b.prefix)
	}
	parts = append(parts, dir, filename)
	return strings.Join(parts, "/")
}

// s3URL returns the canonical s3:// URL for the parquet object.
func (b *S3Backend) s3URL(ref SegmentRef) string {
	return "s3://" + b.bucket + "/" + b.parquetKey(ref)
}

// Archive implements Backend. The flow is:
//   - Compute the parquet SHA-256 if the caller didn't provide one.
//   - HEAD the destination key; if it exists with the same SHA, return
//     ErrAlreadyPresent (catalog still updates the s3_url, so retention
//     can advance).
//   - Upload the parquet (single-part or multipart based on size).
//   - HEAD-verify the SHA tag round-tripped.
//   - Build the manifest with the recorded SHA and write it as the
//     commit marker.
func (b *S3Backend) Archive(ctx context.Context, ref SegmentRef) (ArchiveResult, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if ref.ParquetPath == "" {
		return ArchiveResult{}, errors.New("s3 archive: ParquetPath required")
	}
	st, err := os.Stat(ref.ParquetPath)
	if err != nil {
		return ArchiveResult{}, fmt.Errorf("stat %s: %w", ref.ParquetPath, err)
	}
	sha := ref.ParquetSHA256
	if sha == "" {
		s, _, err := segments.ParquetSHA256(ref.ParquetPath)
		if err != nil {
			return ArchiveResult{}, err
		}
		sha = s
	}

	pkey := b.parquetKey(ref)
	mkey := b.manifestKey(ref)

	// Idempotency: HEAD parquet; if SHA matches we can short-circuit and
	// proceed straight to manifest write (the manifest is the commit
	// marker, so re-writing it makes the segment "committed").
	head, err := b.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(pkey),
	})
	parquetPresent := false
	switch {
	case err == nil:
		parquetPresent = true
		got := head.Metadata[MetadataParquetSHA]
		if got != "" && !strings.EqualFold(got, sha) {
			b.logger.Warn("s3 archive: overwriting parquet with mismatched SHA",
				"segment_id", ref.ID, "key", pkey, "remote_sha", got, "local_sha", sha)
			parquetPresent = false
		}
	default:
		if !isNotFound(err) {
			return ArchiveResult{}, fmt.Errorf("head %s: %w", pkey, err)
		}
	}

	if !parquetPresent {
		if st.Size() > b.multipartThreshold {
			if err := b.uploadMultipart(ctx, pkey, ref.ParquetPath, sha); err != nil {
				return ArchiveResult{}, err
			}
		} else {
			if err := b.uploadSingle(ctx, pkey, ref.ParquetPath, sha); err != nil {
				return ArchiveResult{}, err
			}
		}
		// Re-HEAD to confirm the metadata round-tripped (and the object
		// is durable on the remote side).
		check, err := b.client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(b.bucket),
			Key:    aws.String(pkey),
		})
		if err != nil {
			return ArchiveResult{}, fmt.Errorf("head verify %s: %w", pkey, err)
		}
		if got := check.Metadata[MetadataParquetSHA]; got != "" && !strings.EqualFold(got, sha) {
			return ArchiveResult{}, fmt.Errorf("head verify mismatch for %s: got %q want %q", pkey, got, sha)
		}
	}

	// Build the manifest. The caller may have left ParquetPath as a
	// full path; only the basename appears in the manifest's
	// parquet_filename.
	manifest, err := segments.BuildManifest(
		ref.ID, ref.TimeStart, ref.TimeEnd,
		ref.RowCount, ref.SizeBytes, st.Size(),
		ref.SourceNames, ref.ParquetPath, sha, ref.Persistent,
	)
	if err != nil {
		return ArchiveResult{}, err
	}
	body, err := manifest.Encode()
	if err != nil {
		return ArchiveResult{}, err
	}
	mSHA, _, err := sha256Bytes(body)
	if err != nil {
		return ArchiveResult{}, err
	}

	_, err = b.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(b.bucket),
		Key:         aws.String(mkey),
		Body:        bytes.NewReader(body),
		ContentType: aws.String("application/json"),
		Metadata: map[string]string{
			MetadataParquetSHA: sha,
		},
	})
	if err != nil {
		return ArchiveResult{}, fmt.Errorf("put manifest %s: %w", mkey, err)
	}

	result := ArchiveResult{
		S3URL:          b.s3URL(ref),
		ManifestSHA256: mSHA,
		ParquetSHA256:  sha,
		AlreadyPresent: parquetPresent,
		Manifest:       manifest,
	}
	b.logger.Info("s3 archive: committed",
		"segment_id", ref.ID,
		"key", pkey,
		"manifest_key", mkey,
		"size_bytes", st.Size(),
		"already_present", parquetPresent,
	)
	return result, nil
}

func (b *S3Backend) uploadSingle(ctx context.Context, key, path, sha string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("opening %s: %w", path, err)
	}
	defer f.Close()
	_, err = b.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(b.bucket),
		Key:         aws.String(key),
		Body:        f,
		ContentType: aws.String("application/vnd.apache.parquet"),
		Metadata: map[string]string{
			MetadataParquetSHA: sha,
		},
	})
	if err != nil {
		return fmt.Errorf("put %s: %w", key, err)
	}
	return nil
}

func (b *S3Backend) uploadMultipart(ctx context.Context, key, path, sha string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("opening %s: %w", path, err)
	}
	defer f.Close()

	create, err := b.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket:      aws.String(b.bucket),
		Key:         aws.String(key),
		ContentType: aws.String("application/vnd.apache.parquet"),
		Metadata: map[string]string{
			MetadataParquetSHA: sha,
		},
	})
	if err != nil {
		return fmt.Errorf("create multipart %s: %w", key, err)
	}
	uploadID := aws.ToString(create.UploadId)

	var parts []s3types.CompletedPart
	buf := make([]byte, b.partSize)
	for partNumber := int32(1); ; partNumber++ {
		n, readErr := io.ReadFull(f, buf)
		if n > 0 {
			out, upErr := b.client.UploadPart(ctx, &s3.UploadPartInput{
				Bucket:     aws.String(b.bucket),
				Key:        aws.String(key),
				UploadId:   aws.String(uploadID),
				PartNumber: aws.Int32(partNumber),
				Body:       bytes.NewReader(buf[:n]),
			})
			if upErr != nil {
				_, _ = b.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
					Bucket: aws.String(b.bucket), Key: aws.String(key), UploadId: aws.String(uploadID),
				})
				return fmt.Errorf("upload part %d: %w", partNumber, upErr)
			}
			parts = append(parts, s3types.CompletedPart{
				ETag:       out.ETag,
				PartNumber: aws.Int32(partNumber),
			})
		}
		if errors.Is(readErr, io.EOF) || errors.Is(readErr, io.ErrUnexpectedEOF) {
			break
		}
		if readErr != nil {
			_, _ = b.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
				Bucket: aws.String(b.bucket), Key: aws.String(key), UploadId: aws.String(uploadID),
			})
			return fmt.Errorf("reading %s: %w", path, readErr)
		}
	}

	if len(parts) == 0 {
		_, _ = b.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
			Bucket: aws.String(b.bucket), Key: aws.String(key), UploadId: aws.String(uploadID),
		})
		return fmt.Errorf("multipart upload %s: no parts uploaded", key)
	}

	_, err = b.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(b.bucket),
		Key:             aws.String(key),
		UploadId:        aws.String(uploadID),
		MultipartUpload: &s3types.CompletedMultipartUpload{Parts: parts},
	})
	if err != nil {
		return fmt.Errorf("complete multipart %s: %w", key, err)
	}
	return nil
}

// Retrieve implements Backend. Downloads the parquet and the manifest
// into outDir, validates the SHA-256 against the manifest, and returns
// the local paths.
func (b *S3Backend) Retrieve(ctx context.Context, ref SegmentRef, outDir string) (RetrieveResult, error) {
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return RetrieveResult{}, fmt.Errorf("mkdir %s: %w", outDir, err)
	}

	pkey := b.parquetKey(ref)
	mkey := b.manifestKey(ref)
	parquetPath := filepath.Join(outDir, segments.ParquetFilename(ref.ID))
	manifestPath := filepath.Join(outDir, segments.ManifestFilename(ref.ID))

	if err := b.downloadObject(ctx, mkey, manifestPath); err != nil {
		return RetrieveResult{}, fmt.Errorf("manifest: %w", err)
	}
	manifest, err := segments.ReadManifestFile(manifestPath)
	if err != nil {
		return RetrieveResult{}, err
	}

	if err := b.downloadObject(ctx, pkey, parquetPath); err != nil {
		return RetrieveResult{}, fmt.Errorf("parquet: %w", err)
	}

	sha, _, err := segments.ParquetSHA256(parquetPath)
	if err != nil {
		return RetrieveResult{}, err
	}
	if !strings.EqualFold(sha, manifest.ParquetSHA256) {
		return RetrieveResult{}, fmt.Errorf("retrieved parquet sha mismatch: got %s want %s", sha, manifest.ParquetSHA256)
	}

	return RetrieveResult{
		ParquetPath:  parquetPath,
		ManifestPath: manifestPath,
		Manifest:     manifest,
	}, nil
}

func (b *S3Backend) downloadObject(ctx context.Context, key, dst string) error {
	out, err := b.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("get %s: %w", key, err)
	}
	defer out.Body.Close()
	tmp := dst + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, out.Body); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, dst); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

// Verify implements Backend. Walks the prefix and reports orphan
// parquets, dangling manifests, and missing parquets. When segIDs is
// non-empty the walk is bounded to those ids (manifests/parquets named
// after them); otherwise the entire prefix is scanned.
func (b *S3Backend) Verify(ctx context.Context, segIDs []string) (VerifyResult, error) {
	out := VerifyResult{Backend: "s3"}

	prefix := ""
	if b.prefix != "" {
		prefix = b.prefix + "/"
	}
	prefix += "segments/"

	parquets := map[string]string{}
	manifests := map[string]string{}

	var token *string
	for {
		page, err := b.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(b.bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: token,
		})
		if err != nil {
			return out, fmt.Errorf("list: %w", err)
		}
		for _, obj := range page.Contents {
			key := aws.ToString(obj.Key)
			name := filepath.Base(key)
			switch {
			case strings.HasSuffix(name, ".manifest.json"):
				id := strings.TrimSuffix(strings.TrimPrefix(name, "segment-"), ".manifest.json")
				manifests[id] = key
			case strings.HasSuffix(name, ".parquet"):
				id := strings.TrimSuffix(strings.TrimPrefix(name, "segment-"), ".parquet")
				parquets[id] = key
			}
		}
		if !aws.ToBool(page.IsTruncated) {
			break
		}
		token = page.NextContinuationToken
	}

	wanted := map[string]bool{}
	for _, id := range segIDs {
		wanted[id] = true
	}
	keep := func(id string) bool {
		if len(wanted) == 0 {
			return true
		}
		return wanted[id]
	}

	out.Scanned = len(parquets) + len(manifests)
	for id, key := range parquets {
		if !keep(id) {
			continue
		}
		if _, ok := manifests[id]; !ok {
			out.OrphanedParquets = append(out.OrphanedParquets, key)
		}
	}
	for id, key := range manifests {
		if !keep(id) {
			continue
		}
		if _, ok := parquets[id]; !ok {
			out.DanglingManifests = append(out.DanglingManifests, key)
		}
	}
	return out, nil
}

// isNotFound returns true when the s3 error indicates the key does not
// exist. The aws-sdk-go-v2 error types vary by endpoint; we string-match
// on the canonical "NotFound" and "NoSuchKey" codes.
func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	var nsk *s3types.NoSuchKey
	if errors.As(err, &nsk) {
		return true
	}
	var nfe *s3types.NotFound
	if errors.As(err, &nfe) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "NotFound") || strings.Contains(msg, "NoSuchKey") || strings.Contains(msg, "status code: 404")
}

// ParseS3URL splits an s3://bucket/key URL into its components. Used by
// retrieve paths that begin with the catalog's stored s3_url.
func ParseS3URL(s string) (bucket, key string, err error) {
	u, err := url.Parse(s)
	if err != nil {
		return "", "", err
	}
	if u.Scheme != "s3" {
		return "", "", fmt.Errorf("not an s3 url: %s", s)
	}
	return u.Host, strings.TrimPrefix(u.Path, "/"), nil
}

func sha256Bytes(b []byte) (string, int64, error) {
	h := sha256.New()
	n, err := h.Write(b)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), int64(n), nil
}
