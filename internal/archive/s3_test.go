package archive

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/dokku/logpond/internal/segments"
)

// fakeS3 is an in-memory S3 implementation good enough to exercise the
// archive backend's PutObject / HeadObject / GetObject / List path plus
// the multipart upload sequence.
type fakeS3 struct {
	mu       sync.Mutex
	objects  map[string]fakeObject
	uploads  map[string]map[int32][]byte
	uploadID int

	// Optional fault injection: when set to non-empty, the operation
	// named here returns errFail.
	failOp string
}

type fakeObject struct {
	body     []byte
	metadata map[string]string
}

func newFakeS3() *fakeS3 {
	return &fakeS3{
		objects: map[string]fakeObject{},
		uploads: map[string]map[int32][]byte{},
	}
}

var errFail = errors.New("fake s3: injected failure")

func (f *fakeS3) HeadObject(_ context.Context, in *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failOp == "HeadObject" {
		return nil, errFail
	}
	key := keyOf(in.Bucket, in.Key)
	obj, ok := f.objects[key]
	if !ok {
		return nil, &s3types.NotFound{Message: aws.String("not found")}
	}
	return &s3.HeadObjectOutput{
		ContentLength: aws.Int64(int64(len(obj.body))),
		Metadata:      cloneMeta(obj.metadata),
	}, nil
}

func (f *fakeS3) PutObject(_ context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failOp == "PutObject" {
		return nil, errFail
	}
	buf, err := io.ReadAll(in.Body)
	if err != nil {
		return nil, err
	}
	key := keyOf(in.Bucket, in.Key)
	f.objects[key] = fakeObject{
		body:     buf,
		metadata: cloneMeta(in.Metadata),
	}
	return &s3.PutObjectOutput{}, nil
}

func (f *fakeS3) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failOp == "GetObject" {
		return nil, errFail
	}
	key := keyOf(in.Bucket, in.Key)
	obj, ok := f.objects[key]
	if !ok {
		return nil, &s3types.NoSuchKey{Message: aws.String("not found")}
	}
	return &s3.GetObjectOutput{
		Body:          io.NopCloser(bytes.NewReader(obj.body)),
		ContentLength: aws.Int64(int64(len(obj.body))),
		Metadata:      cloneMeta(obj.metadata),
	}, nil
}

func (f *fakeS3) ListObjectsV2(_ context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	prefix := aws.ToString(in.Prefix)
	bucket := aws.ToString(in.Bucket)
	keys := []string{}
	for k := range f.objects {
		if !strings.HasPrefix(k, bucket+"/") {
			continue
		}
		rel := strings.TrimPrefix(k, bucket+"/")
		if strings.HasPrefix(rel, prefix) {
			keys = append(keys, rel)
		}
	}
	sort.Strings(keys)
	out := &s3.ListObjectsV2Output{
		Name:        aws.String(bucket),
		Prefix:      in.Prefix,
		IsTruncated: aws.Bool(false),
	}
	for _, k := range keys {
		out.Contents = append(out.Contents, s3types.Object{Key: aws.String(k)})
	}
	return out, nil
}

func (f *fakeS3) CreateMultipartUpload(_ context.Context, in *s3.CreateMultipartUploadInput, _ ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.uploadID++
	id := fmt.Sprintf("u%d", f.uploadID)
	f.uploads[id] = map[int32][]byte{}
	// Persist the create-time metadata so Complete can attach it.
	key := keyOf(in.Bucket, in.Key) + "::pending"
	f.objects[key] = fakeObject{metadata: cloneMeta(in.Metadata)}
	return &s3.CreateMultipartUploadOutput{UploadId: aws.String(id)}, nil
}

func (f *fakeS3) UploadPart(_ context.Context, in *s3.UploadPartInput, _ ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	buf, err := io.ReadAll(in.Body)
	if err != nil {
		return nil, err
	}
	upload, ok := f.uploads[aws.ToString(in.UploadId)]
	if !ok {
		return nil, errors.New("unknown upload")
	}
	upload[aws.ToInt32(in.PartNumber)] = buf
	sum := md5.Sum(buf)
	return &s3.UploadPartOutput{ETag: aws.String(hex.EncodeToString(sum[:]))}, nil
}

func (f *fakeS3) CompleteMultipartUpload(_ context.Context, in *s3.CompleteMultipartUploadInput, _ ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	upload, ok := f.uploads[aws.ToString(in.UploadId)]
	if !ok {
		return nil, errors.New("unknown upload")
	}
	// Reassemble parts in part-number order.
	keys := []int{}
	for k := range upload {
		keys = append(keys, int(k))
	}
	sort.Ints(keys)
	var buf bytes.Buffer
	for _, k := range keys {
		buf.Write(upload[int32(k)])
	}
	pending := keyOf(in.Bucket, in.Key) + "::pending"
	meta := f.objects[pending].metadata
	delete(f.objects, pending)
	delete(f.uploads, aws.ToString(in.UploadId))
	key := keyOf(in.Bucket, in.Key)
	f.objects[key] = fakeObject{body: buf.Bytes(), metadata: cloneMeta(meta)}
	return &s3.CompleteMultipartUploadOutput{}, nil
}

func (f *fakeS3) AbortMultipartUpload(_ context.Context, in *s3.AbortMultipartUploadInput, _ ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.uploads, aws.ToString(in.UploadId))
	delete(f.objects, keyOf(in.Bucket, in.Key)+"::pending")
	return &s3.AbortMultipartUploadOutput{}, nil
}

func keyOf(bucket, key *string) string {
	return aws.ToString(bucket) + "/" + aws.ToString(key)
}

func cloneMeta(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func writeFile(t *testing.T, dir, name string, data []byte) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return path
}

func sha256Hex(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func ref(t *testing.T, parquet string, sha string) SegmentRef {
	t.Helper()
	st, err := os.Stat(parquet)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	return SegmentRef{
		ID:            "202605121400",
		TimeStart:     time.Date(2026, 5, 12, 14, 0, 0, 0, time.UTC),
		TimeEnd:       time.Date(2026, 5, 12, 15, 0, 0, 0, time.UTC),
		RowCount:      1,
		SizeBytes:     st.Size(),
		ParquetPath:   parquet,
		ParquetSHA256: sha,
		SourceNames:   []string{"default"},
	}
}

func TestS3Backend_Archive_PutsParquetAndManifest(t *testing.T) {
	dir := t.TempDir()
	body := []byte("hello-parquet")
	parquet := writeFile(t, dir, "segment-202605121400.parquet", body)
	fake := newFakeS3()
	b, err := NewS3Backend(S3Options{Client: fake, Bucket: "bkt", Prefix: "logpond/"})
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	res, err := b.Archive(context.Background(), ref(t, parquet, sha256Hex(body)))
	if err != nil {
		t.Fatalf("archive: %v", err)
	}
	if res.AlreadyPresent {
		t.Fatal("first archive should not report AlreadyPresent")
	}
	wantPKey := "bkt/logpond/segments/2026/05/12/14/segment-202605121400.parquet"
	if _, ok := fake.objects[wantPKey]; !ok {
		t.Fatalf("expected parquet at %s, have %v", wantPKey, mapKeys(fake.objects))
	}
	wantMKey := "bkt/logpond/segments/2026/05/12/14/segment-202605121400.manifest.json"
	if _, ok := fake.objects[wantMKey]; !ok {
		t.Fatalf("expected manifest at %s", wantMKey)
	}
	if res.S3URL == "" {
		t.Fatal("expected populated S3URL")
	}
	// Verify the metadata SHA round-tripped.
	if got := fake.objects[wantPKey].metadata["parquet-sha256"]; got != sha256Hex(body) {
		t.Fatalf("metadata sha mismatch: %s", got)
	}
}

func TestS3Backend_Archive_NoOpOnMatchingSHA(t *testing.T) {
	dir := t.TempDir()
	body := []byte("idempotent-payload")
	parquet := writeFile(t, dir, "segment-202605121400.parquet", body)
	fake := newFakeS3()
	b, err := NewS3Backend(S3Options{Client: fake, Bucket: "bkt"})
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	ctx := context.Background()
	r := ref(t, parquet, sha256Hex(body))
	if _, err := b.Archive(ctx, r); err != nil {
		t.Fatalf("first: %v", err)
	}
	// Re-archive. Should short-circuit the parquet upload but rewrite
	// the manifest (commit marker semantics).
	res2, err := b.Archive(ctx, r)
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if !res2.AlreadyPresent {
		t.Fatal("second archive should report AlreadyPresent")
	}
}

func TestS3Backend_Archive_OverwriteOnSHAMismatch(t *testing.T) {
	dir := t.TempDir()
	bodyOld := []byte("old-bytes")
	bodyNew := []byte("new-bytes-different-length")
	parquet := writeFile(t, dir, "segment-202605121400.parquet", bodyOld)
	fake := newFakeS3()
	b, err := NewS3Backend(S3Options{Client: fake, Bucket: "bkt"})
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	ctx := context.Background()
	r := ref(t, parquet, sha256Hex(bodyOld))
	if _, err := b.Archive(ctx, r); err != nil {
		t.Fatalf("first: %v", err)
	}
	// Replace the file with new bytes; archive again with the new SHA.
	if err := os.WriteFile(parquet, bodyNew, 0o644); err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	r2 := ref(t, parquet, sha256Hex(bodyNew))
	r2.ID = r.ID
	res, err := b.Archive(ctx, r2)
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if res.AlreadyPresent {
		t.Fatal("mismatched sha should not report AlreadyPresent")
	}
	pKey := "bkt/segments/2026/05/12/14/segment-202605121400.parquet"
	if got := fake.objects[pKey].metadata["parquet-sha256"]; got != sha256Hex(bodyNew) {
		t.Fatalf("expected updated sha, got %s", got)
	}
	if !bytes.Equal(fake.objects[pKey].body, bodyNew) {
		t.Fatal("expected body to be overwritten")
	}
}

func TestS3Backend_Archive_Multipart(t *testing.T) {
	dir := t.TempDir()
	// Force multipart by lowering thresholds. Payload is 200 bytes, parts
	// of 64 → three parts.
	body := bytes.Repeat([]byte("x"), 200)
	parquet := writeFile(t, dir, "segment-202605121400.parquet", body)
	fake := newFakeS3()
	b, err := NewS3Backend(S3Options{
		Client:             fake,
		Bucket:             "bkt",
		MultipartThreshold: 100,
		PartSize:           64,
	})
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	res, err := b.Archive(context.Background(), ref(t, parquet, sha256Hex(body)))
	if err != nil {
		t.Fatalf("archive: %v", err)
	}
	if res.AlreadyPresent {
		t.Fatal("first archive should not report AlreadyPresent")
	}
	pKey := "bkt/segments/2026/05/12/14/segment-202605121400.parquet"
	got := fake.objects[pKey].body
	if !bytes.Equal(got, body) {
		t.Fatalf("multipart reassembly mismatch: len got=%d want=%d", len(got), len(body))
	}
	if meta := fake.objects[pKey].metadata["parquet-sha256"]; meta != sha256Hex(body) {
		t.Fatalf("expected sha metadata, got %q", meta)
	}
}

func TestS3Backend_Retrieve_RoundTrips(t *testing.T) {
	dir := t.TempDir()
	body := []byte("retrieve-me")
	parquet := writeFile(t, dir, "segment-202605121400.parquet", body)
	fake := newFakeS3()
	b, err := NewS3Backend(S3Options{Client: fake, Bucket: "bkt", Prefix: "p"})
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	ctx := context.Background()
	if _, err := b.Archive(ctx, ref(t, parquet, sha256Hex(body))); err != nil {
		t.Fatalf("archive: %v", err)
	}
	outDir := filepath.Join(dir, "rehydrate")
	res, err := b.Retrieve(ctx, ref(t, parquet, sha256Hex(body)), outDir)
	if err != nil {
		t.Fatalf("retrieve: %v", err)
	}
	if res.ParquetPath == "" || res.ManifestPath == "" {
		t.Fatal("retrieve paths should be set")
	}
	got, err := os.ReadFile(res.ParquetPath)
	if err != nil {
		t.Fatalf("read parquet: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Fatal("retrieved parquet body mismatch")
	}
	if res.Manifest.ParquetSHA256 != sha256Hex(body) {
		t.Fatalf("manifest sha mismatch: %s", res.Manifest.ParquetSHA256)
	}
}

func TestS3Backend_Verify_ReportsOrphansAndDangling(t *testing.T) {
	dir := t.TempDir()
	body := []byte("verify-payload")
	parquet := writeFile(t, dir, "segment-202605121400.parquet", body)
	fake := newFakeS3()
	b, err := NewS3Backend(S3Options{Client: fake, Bucket: "bkt"})
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	ctx := context.Background()
	if _, err := b.Archive(ctx, ref(t, parquet, sha256Hex(body))); err != nil {
		t.Fatalf("archive: %v", err)
	}
	// Drop the manifest to make the parquet an orphan.
	mKey := "bkt/segments/2026/05/12/14/" + segments.ManifestFilename("202605121400")
	delete(fake.objects, mKey)
	res, err := b.Verify(ctx, nil)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if len(res.OrphanedParquets) != 1 {
		t.Fatalf("expected one orphan, got %v", res.OrphanedParquets)
	}
	// Add a dangling manifest pointing nowhere.
	fake.objects["bkt/segments/2026/05/12/15/segment-other.manifest.json"] = fakeObject{}
	res, err = b.Verify(ctx, nil)
	if err != nil {
		t.Fatalf("verify2: %v", err)
	}
	if len(res.DanglingManifests) != 1 {
		t.Fatalf("expected one dangling manifest, got %v", res.DanglingManifests)
	}
}

func mapKeys(m map[string]fakeObject) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
