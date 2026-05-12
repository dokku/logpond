package api

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dokku/logpond/internal/catalog"
	"github.com/dokku/logpond/internal/ingest"
	"github.com/dokku/logpond/internal/jobs"
	"github.com/dokku/logpond/internal/metrics"
	"github.com/dokku/logpond/internal/segments"
)

func newImportServer(t *testing.T) (*Server, *catalog.Catalog, string) {
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
		Jobs:           jm,
		DataDir:        dir,
		RehydrationTTL: 7 * 24 * time.Hour,
	})
	return srv, cat, dir
}

func buildMultipartBody(t *testing.T, parquet, manifest []byte) (*bytes.Buffer, string) {
	t.Helper()
	body := &bytes.Buffer{}
	mw := multipart.NewWriter(body)
	pw, err := mw.CreateFormFile("parquet", "segment.parquet")
	if err != nil {
		t.Fatalf("create parquet part: %v", err)
	}
	if _, err := pw.Write(parquet); err != nil {
		t.Fatalf("write parquet: %v", err)
	}
	mfw, err := mw.CreateFormFile("manifest", "manifest.json")
	if err != nil {
		t.Fatalf("create manifest part: %v", err)
	}
	if _, err := mfw.Write(manifest); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	if err := mw.Close(); err != nil {
		t.Fatalf("close mw: %v", err)
	}
	return body, mw.FormDataContentType()
}

func sampleManifest(id string, parquet []byte, start time.Time) segments.Manifest {
	return segments.Manifest{
		ManifestVersion:  segments.ManifestVersion,
		SchemaVersion:    1,
		SegmentID:        id,
		TimeStart:        start,
		TimeEnd:          start.Add(time.Hour),
		RowCount:         100,
		SizeBytes:        int64(len(parquet)),
		SizeCompressed:   int64(len(parquet)),
		Compression:      "zstd",
		CompressionLevel: 3,
		SourceNames:      []string{"default"},
		ParquetFilename:  segments.ParquetFilename(id),
		ParquetSHA256:    sha256Hex(parquet),
		Persistent:       false,
	}
}

func TestImport_HappyPath(t *testing.T) {
	srv, cat, dataDir := newImportServer(t)
	parquet := []byte("parquet-content")
	start := time.Date(2026, 4, 12, 1, 0, 0, 0, time.UTC)
	m := sampleManifest("202604120100", parquet, start)
	manifestBuf, err := m.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	body, ct := buildMultipartBody(t, parquet, manifestBuf)
	req := httptest.NewRequest(http.MethodPost, "/api/import", body)
	req.Header.Set("Content-Type", ct)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusCreated {
		t.Fatalf("status: %d body=%s", rr.Code, rr.Body.String())
	}
	var resp importResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.SegmentID != "202604120100" {
		t.Fatalf("segment_id: %s", resp.SegmentID)
	}
	if resp.State != catalog.StateRehydrated {
		t.Fatalf("state: %s", resp.State)
	}
	if resp.EvictAfter == nil {
		t.Fatalf("evict_after should be set for non-persistent import")
	}
	parquetPath := filepath.Join(dataDir, "rehydrated", segments.ParquetFilename("202604120100"))
	if _, err := os.Stat(parquetPath); err != nil {
		t.Fatalf("parquet not staged: %v", err)
	}
	seg, _ := cat.GetSegment(context.Background(), "202604120100")
	if seg.State != catalog.StateRehydrated {
		t.Fatalf("catalog state: %s", seg.State)
	}
}

func TestImport_BadSHA_400(t *testing.T) {
	srv, _, _ := newImportServer(t)
	parquet := []byte("parquet-content")
	m := sampleManifest("seg-x", parquet, time.Now().UTC().Truncate(time.Hour))
	m.ParquetSHA256 = "deadbeef" // mismatch
	buf, _ := m.Encode()
	body, ct := buildMultipartBody(t, parquet, buf)
	req := httptest.NewRequest(http.MethodPost, "/api/import", body)
	req.Header.Set("Content-Type", ct)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status: %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestImport_DuplicateID_409(t *testing.T) {
	srv, cat, _ := newImportServer(t)
	id := "dup-1"
	if err := cat.InsertSegment(context.Background(), catalog.Segment{
		ID:        id,
		State:     catalog.StateSealed,
		TimeStart: time.Now().UTC().Add(-time.Hour),
		TimeEnd:   time.Now().UTC(),
		RowCount:  1,
		SizeBytes: 1,
		CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	parquet := []byte("payload")
	m := sampleManifest(id, parquet, time.Now().UTC().Truncate(time.Hour))
	buf, _ := m.Encode()
	body, ct := buildMultipartBody(t, parquet, buf)
	req := httptest.NewRequest(http.MethodPost, "/api/import", body)
	req.Header.Set("Content-Type", ct)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusConflict {
		t.Fatalf("status: %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestImport_OverlapPopulated(t *testing.T) {
	srv, cat, _ := newImportServer(t)
	start := time.Date(2026, 4, 12, 0, 30, 0, 0, time.UTC)
	// Pre-register a segment overlapping the import target window
	// (00:00-01:00).
	if err := cat.InsertSegment(context.Background(), catalog.Segment{
		ID:        "overlapper",
		State:     catalog.StateSealed,
		TimeStart: start.Add(-30 * time.Minute), // 00:00
		TimeEnd:   start.Add(30 * time.Minute),  // 01:00
		RowCount:  1,
		SizeBytes: 1,
		CreatedAt: start,
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	parquet := []byte("payload-overlap")
	m := sampleManifest("imported-1", parquet, time.Date(2026, 4, 12, 0, 0, 0, 0, time.UTC))
	buf, _ := m.Encode()
	body, ct := buildMultipartBody(t, parquet, buf)
	req := httptest.NewRequest(http.MethodPost, "/api/import", body)
	req.Header.Set("Content-Type", ct)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusCreated {
		t.Fatalf("status: %d body=%s", rr.Code, rr.Body.String())
	}
	var resp importResponse
	_ = json.Unmarshal(rr.Body.Bytes(), &resp)
	if len(resp.Overlaps) != 1 || resp.Overlaps[0].SegmentID != "overlapper" {
		t.Fatalf("overlaps: %+v", resp.Overlaps)
	}
}

func TestImport_PersistentQueryParam(t *testing.T) {
	srv, _, _ := newImportServer(t)
	parquet := []byte("payload-persistent")
	m := sampleManifest("pers-1", parquet, time.Now().UTC().Truncate(time.Hour))
	buf, _ := m.Encode()
	body, ct := buildMultipartBody(t, parquet, buf)
	req := httptest.NewRequest(http.MethodPost, "/api/import?persistent=true", body)
	req.Header.Set("Content-Type", ct)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusCreated {
		t.Fatalf("status: %d body=%s", rr.Code, rr.Body.String())
	}
	var resp importResponse
	_ = json.Unmarshal(rr.Body.Bytes(), &resp)
	if !resp.Persistent {
		t.Fatalf("persistent flag not set: %+v", resp)
	}
	if resp.EvictAfter != nil {
		t.Fatalf("evict_after should be null for persistent import")
	}
}

// Quick guard that buildMultipartBody is wired up correctly (so
// failures elsewhere aren't masquerading as transport issues).
func TestImport_HelperRoundtrips(t *testing.T) {
	parquet := []byte("hello")
	manifest := []byte(`{"k":"v"}`)
	body, ct := buildMultipartBody(t, parquet, manifest)
	req := httptest.NewRequest(http.MethodPost, "/", body)
	req.Header.Set("Content-Type", ct)
	if err := req.ParseMultipartForm(1 << 20); err != nil {
		t.Fatalf("parse multipart: %v", err)
	}
	p, _, err := req.FormFile("parquet")
	if err != nil {
		t.Fatalf("parquet missing: %v", err)
	}
	buf, _ := io.ReadAll(p)
	if !bytes.Equal(buf, parquet) {
		t.Fatalf("parquet bytes mismatch")
	}
}

func TestImportWatcher_HappyPath(t *testing.T) {
	srv, cat, dataDir := newImportServer(t)
	_ = srv
	importDir := filepath.Join(dataDir, "import")
	if err := os.MkdirAll(importDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	parquet := []byte("payload-watch")
	id := "watch-1"
	stem := "segment-" + id
	if err := os.WriteFile(filepath.Join(importDir, stem+".parquet"), parquet, 0o644); err != nil {
		t.Fatalf("write parquet: %v", err)
	}
	m := sampleManifest(id, parquet, time.Now().UTC().Truncate(time.Hour))
	buf, _ := m.Encode()
	if err := os.WriteFile(filepath.Join(importDir, stem+".manifest.json"), buf, 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	if err := os.WriteFile(filepath.Join(importDir, stem+".ready"), []byte("go"), 0o644); err != nil {
		t.Fatalf("write ready: %v", err)
	}

	w := NewImportWatcher(ImportWatcherOptions{
		DataDir: dataDir,
		Catalog: cat,
		TTL:     7 * 24 * time.Hour,
	})
	if err := w.Sweep(context.Background()); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	seg, err := cat.GetSegment(context.Background(), id)
	if err != nil {
		t.Fatalf("get segment: %v", err)
	}
	if seg.State != catalog.StateRehydrated {
		t.Fatalf("state: %s", seg.State)
	}
	if _, err := os.Stat(filepath.Join(dataDir, "import", "imported", id, stem+".ready")); err != nil {
		t.Fatalf("ready not moved to imported/: %v", err)
	}
}

func TestImportWatcher_BadSHA_MovesToFailed(t *testing.T) {
	_, cat, dataDir := func() (*Server, *catalog.Catalog, string) {
		dir := t.TempDir()
		c, err := catalog.Open(context.Background(), filepath.Join(dir, "catalog.db"), nil)
		if err != nil {
			t.Fatalf("catalog: %v", err)
		}
		t.Cleanup(func() { _ = c.Close() })
		return nil, c, dir
	}()
	importDir := filepath.Join(dataDir, "import")
	if err := os.MkdirAll(importDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	stem := "segment-bad-1"
	if err := os.WriteFile(filepath.Join(importDir, stem+".parquet"), []byte("data"), 0o644); err != nil {
		t.Fatalf("write parquet: %v", err)
	}
	m := sampleManifest("bad-1", []byte("data"), time.Now().UTC().Truncate(time.Hour))
	m.ParquetSHA256 = "deadbeef"
	buf, _ := m.Encode()
	if err := os.WriteFile(filepath.Join(importDir, stem+".manifest.json"), buf, 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	if err := os.WriteFile(filepath.Join(importDir, stem+".ready"), []byte("go"), 0o644); err != nil {
		t.Fatalf("write ready: %v", err)
	}
	w := NewImportWatcher(ImportWatcherOptions{
		DataDir: dataDir,
		Catalog: cat,
		TTL:     time.Hour,
	})
	_ = w.Sweep(context.Background())
	failedReady := filepath.Join(dataDir, "import", "failed", stem, stem+".ready")
	if _, err := os.Stat(failedReady); err != nil {
		t.Fatalf("ready not moved to failed/: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dataDir, "import", "failed", stem, "error.txt")); err != nil {
		t.Fatalf("error.txt missing: %v", err)
	}
	// Catalog should be untouched.
	if _, err := cat.GetSegment(context.Background(), "bad-1"); err == nil {
		t.Fatalf("bad segment should not be registered")
	} else if err != sql.ErrNoRows {
		t.Fatalf("unexpected err: %v", err)
	}
}
