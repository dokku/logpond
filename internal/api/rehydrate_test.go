package api

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dokku/logpond/internal/archive"
	"github.com/dokku/logpond/internal/catalog"
	"github.com/dokku/logpond/internal/ingest"
	"github.com/dokku/logpond/internal/jobs"
	"github.com/dokku/logpond/internal/metrics"
	"github.com/dokku/logpond/internal/segments"
)

// retrieveBackend stages parquet/manifest files into the requested out
// dir so the rehydrate handler can validate the SHA round-trip.
type retrieveBackend struct {
	caps           archive.Capabilities
	parquetContent map[string][]byte
	manifests      map[string]segments.Manifest
	failNext       bool
}

func newRetrieveBackend() *retrieveBackend {
	return &retrieveBackend{
		caps:           archive.Capabilities{Name: "s3", Archive: true, Retrieve: true, Verify: true},
		parquetContent: map[string][]byte{},
		manifests:      map[string]segments.Manifest{},
	}
}

func (b *retrieveBackend) Capabilities() archive.Capabilities { return b.caps }
func (b *retrieveBackend) CapabilityDetail() archive.CapabilityDetail {
	return archive.CapabilityDetail{Backend: b.caps.Name, Archive: "yes", Retrieve: "yes", Verify: "yes"}
}
func (b *retrieveBackend) Archive(context.Context, archive.SegmentRef) (archive.ArchiveResult, error) {
	return archive.ArchiveResult{}, nil
}
func (b *retrieveBackend) Verify(context.Context, []string) (archive.VerifyResult, error) {
	return archive.VerifyResult{Backend: b.caps.Name}, nil
}
func (b *retrieveBackend) Retrieve(_ context.Context, ref archive.SegmentRef, outDir string) (archive.RetrieveResult, error) {
	if b.failNext {
		b.failNext = false
		return archive.RetrieveResult{}, archive.ErrUnsupported
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return archive.RetrieveResult{}, err
	}
	parquetPath := filepath.Join(outDir, segments.ParquetFilename(ref.ID))
	if err := os.WriteFile(parquetPath, b.parquetContent[ref.ID], 0o644); err != nil {
		return archive.RetrieveResult{}, err
	}
	manifestPath := filepath.Join(outDir, segments.ManifestFilename(ref.ID))
	if err := segments.WriteManifestFile(b.manifests[ref.ID], manifestPath); err != nil {
		return archive.RetrieveResult{}, err
	}
	return archive.RetrieveResult{
		ParquetPath:  parquetPath,
		ManifestPath: manifestPath,
		Manifest:     b.manifests[ref.ID],
	}, nil
}

func newRehydrateServer(t *testing.T, be archive.Backend) (*Server, *catalog.Catalog, string) {
	t.Helper()
	dir := t.TempDir()
	cat, err := catalog.Open(context.Background(), filepath.Join(dir, "catalog.db"), nil)
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	t.Cleanup(func() { _ = cat.Close() })
	jm, err := jobs.New(jobs.Options{Catalog: cat})
	if err != nil {
		t.Fatalf("jobs: %v", err)
	}
	buf := ingest.NewBuffer(100)
	m := metrics.New(metrics.Options{FillRatio: buf.FillRatio})
	srv := New(Options{
		Buffer:         buf,
		Metrics:        m,
		Extractors:     map[string]*ingest.Extractor{},
		Catalog:        cat,
		ArchiveBackend: be,
		Jobs:           jm,
		DataDir:        dir,
		RehydrationTTL: 7 * 24 * time.Hour,
	})
	return srv, cat, dir
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func registerArchivedSegment(t *testing.T, cat *catalog.Catalog, be *retrieveBackend, id string, start, end time.Time, content []byte) {
	t.Helper()
	be.parquetContent[id] = content
	be.manifests[id] = segments.Manifest{
		ManifestVersion:  segments.ManifestVersion,
		SchemaVersion:    1,
		SegmentID:        id,
		TimeStart:        start,
		TimeEnd:          end,
		RowCount:         42,
		SizeBytes:        int64(len(content)),
		SizeCompressed:   int64(len(content)),
		Compression:      "zstd",
		CompressionLevel: 3,
		SourceNames:      []string{"default"},
		ParquetFilename:  segments.ParquetFilename(id),
		ParquetSHA256:    sha256Hex(content),
		Persistent:       false,
	}
	if err := cat.InsertSegment(context.Background(), catalog.Segment{
		ID:         id,
		State:      catalog.StateArchived,
		TimeStart:  start,
		TimeEnd:    end,
		RowCount:   42,
		SizeBytes:  int64(len(content)),
		S3URL:      sql.NullString{String: "s3://bkt/" + id, Valid: true},
		ArchivedAt: sql.NullTime{Time: end.Add(time.Hour), Valid: true},
		CreatedAt:  start,
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}
}

func waitForState(t *testing.T, cat *catalog.Catalog, id, state string) catalog.Segment {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		seg, err := cat.GetSegment(context.Background(), id)
		if err == nil && seg.State == state {
			return seg
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("segment %s never reached state %s", id, state)
	return catalog.Segment{}
}

func TestRehydrate_HappyPath(t *testing.T) {
	be := newRetrieveBackend()
	srv, cat, dataDir := newRehydrateServer(t, be)

	start := time.Date(2026, 4, 12, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	content := []byte("parquet-payload")
	registerArchivedSegment(t, cat, be, "202604120000", start, end, content)

	body := `{"time_range":{"from":"2026-04-12T00:00:00Z","to":"2026-04-13T00:00:00Z"}}`
	rr := postJSON(t, srv, "/api/rehydrate", body)
	if rr.Code != http.StatusAccepted {
		t.Fatalf("status: %d body=%s", rr.Code, rr.Body.String())
	}
	var resp rehydrateResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.SegmentCount != 1 || resp.Segments[0] != "202604120000" {
		t.Fatalf("segments: %+v", resp)
	}

	seg := waitForState(t, cat, "202604120000", catalog.StateRehydrated)
	if !seg.LocalPath.Valid {
		t.Fatalf("local_path not set: %+v", seg)
	}
	wantPath := filepath.Join(dataDir, "rehydrated", segments.ParquetFilename("202604120000"))
	if seg.LocalPath.String != wantPath {
		t.Fatalf("local_path: got %s want %s", seg.LocalPath.String, wantPath)
	}
	if !seg.EvictAfter.Valid {
		t.Fatalf("evict_after should be set for non-persistent rehydrate")
	}
}

func TestRehydrate_RetrieveUnsupported_501(t *testing.T) {
	be := newRetrieveBackend()
	be.caps = archive.Capabilities{Name: "script", Archive: true, Retrieve: false, Verify: false}
	srv, _, _ := newRehydrateServer(t, be)
	body := `{"time_range":{"from":"2026-04-12T00:00:00Z","to":"2026-04-13T00:00:00Z"}}`
	rr := postJSON(t, srv, "/api/rehydrate", body)
	if rr.Code != http.StatusNotImplemented {
		t.Fatalf("status: %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestRehydrate_Persistent_LeavesEvictAfterNull(t *testing.T) {
	be := newRetrieveBackend()
	srv, cat, _ := newRehydrateServer(t, be)
	start := time.Date(2026, 4, 12, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	content := []byte("payload-2")
	registerArchivedSegment(t, cat, be, "202604120100", start, end, content)
	body := `{"time_range":{"from":"2026-04-12T00:00:00Z","to":"2026-04-13T00:00:00Z"},"persistent":true}`
	rr := postJSON(t, srv, "/api/rehydrate", body)
	if rr.Code != http.StatusAccepted {
		t.Fatalf("status: %d body=%s", rr.Code, rr.Body.String())
	}
	seg := waitForState(t, cat, "202604120100", catalog.StateRehydrated)
	if seg.EvictAfter.Valid {
		t.Fatalf("evict_after should be NULL for persistent rehydrate: %+v", seg.EvictAfter)
	}
}

func TestRehydrate_AlreadyLocal_BumpsTTL(t *testing.T) {
	be := newRetrieveBackend()
	srv, cat, dataDir := newRehydrateServer(t, be)

	start := time.Date(2026, 4, 12, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	id := "202604120200"

	rehydratedDir := filepath.Join(dataDir, "rehydrated")
	if err := os.MkdirAll(rehydratedDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	parquetPath := filepath.Join(rehydratedDir, segments.ParquetFilename(id))
	if err := os.WriteFile(parquetPath, []byte("payload"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	originalEvict := time.Now().UTC().Add(time.Hour)
	if err := cat.InsertSegment(context.Background(), catalog.Segment{
		ID:         id,
		State:      catalog.StateRehydrated,
		TimeStart:  start,
		TimeEnd:    end,
		RowCount:   1,
		SizeBytes:  7,
		LocalPath:  sql.NullString{String: parquetPath, Valid: true},
		S3URL:      sql.NullString{String: "s3://bkt/" + id, Valid: true},
		EvictAfter: sql.NullTime{Time: originalEvict, Valid: true},
		CreatedAt:  start,
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	body := `{"time_range":{"from":"2026-04-12T00:00:00Z","to":"2026-04-12T01:00:00Z"}}`
	rr := postJSON(t, srv, "/api/rehydrate", body)
	if rr.Code != http.StatusAccepted {
		t.Fatalf("status: %d body=%s", rr.Code, rr.Body.String())
	}
	// Wait for the job to settle.
	time.Sleep(150 * time.Millisecond)
	seg, _ := cat.GetSegment(context.Background(), id)
	if !seg.EvictAfter.Valid {
		t.Fatalf("evict_after should remain set")
	}
	if !seg.EvictAfter.Time.After(originalEvict) {
		t.Fatalf("evict_after not bumped: was %s now %s", originalEvict, seg.EvictAfter.Time)
	}
}
