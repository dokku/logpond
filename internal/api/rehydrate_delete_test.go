package api

import (
	"context"
	"database/sql"
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
	"github.com/dokku/logpond/internal/query"
	"github.com/dokku/logpond/internal/segments"
)

func newDeleteServer(t *testing.T) (*Server, *catalog.Catalog, *query.Executor, string) {
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
	exec := query.NewExecutor(cat, nil, nil)
	buf := ingest.NewBuffer(100)
	m := metrics.New(metrics.Options{FillRatio: buf.FillRatio})
	srv := New(Options{
		Buffer:         buf,
		Metrics:        m,
		Extractors:     map[string]*ingest.Extractor{},
		Catalog:        cat,
		Jobs:           jm,
		Executor:       exec,
		DataDir:        dir,
		RehydrationTTL: 7 * 24 * time.Hour,
	})
	return srv, cat, exec, dir
}

func insertRehydrated(t *testing.T, cat *catalog.Catalog, dataDir, id string) string {
	t.Helper()
	rehydratedDir := filepath.Join(dataDir, "rehydrated")
	if err := os.MkdirAll(rehydratedDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	parquet := filepath.Join(rehydratedDir, segments.ParquetFilename(id))
	if err := os.WriteFile(parquet, []byte("payload"), 0o644); err != nil {
		t.Fatalf("write parquet: %v", err)
	}
	manifest := filepath.Join(rehydratedDir, segments.ManifestFilename(id))
	if err := os.WriteFile(manifest, []byte("{}"), 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	now := time.Now().UTC()
	if err := cat.InsertSegment(context.Background(), catalog.Segment{
		ID:         id,
		State:      catalog.StateRehydrated,
		TimeStart:  now.Add(-time.Hour),
		TimeEnd:    now,
		RowCount:   1,
		SizeBytes:  7,
		LocalPath:  sql.NullString{String: parquet, Valid: true},
		S3URL:      sql.NullString{String: "s3://bkt/" + id, Valid: true},
		EvictAfter: sql.NullTime{Time: now.Add(time.Hour), Valid: true},
		CreatedAt:  now.Add(-time.Hour),
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	return parquet
}

func TestDeleteRehydrated_HappyPath(t *testing.T) {
	srv, cat, _, dataDir := newDeleteServer(t)
	parquet := insertRehydrated(t, cat, dataDir, "del-1")
	req := httptest.NewRequest(http.MethodDelete, "/api/rehydrated/del-1", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("status: %d body=%s", rr.Code, rr.Body.String())
	}
	if _, err := os.Stat(parquet); !os.IsNotExist(err) {
		t.Fatalf("parquet still exists: %v", err)
	}
	seg, _ := cat.GetSegment(context.Background(), "del-1")
	if seg.State != catalog.StateArchived {
		t.Fatalf("state: %s", seg.State)
	}
	if seg.LocalPath.Valid {
		t.Fatalf("local_path still set: %s", seg.LocalPath.String)
	}
	if seg.EvictAfter.Valid {
		t.Fatalf("evict_after should be cleared")
	}
}

func TestDeleteRehydrated_404OnMissing(t *testing.T) {
	srv, _, _, _ := newDeleteServer(t)
	req := httptest.NewRequest(http.MethodDelete, "/api/rehydrated/nope", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("status: %d", rr.Code)
	}
}

func TestDeleteRehydrated_409WhenBusy(t *testing.T) {
	srv, cat, exec, dataDir := newDeleteServer(t)
	_ = insertRehydrated(t, cat, dataDir, "busy-1")

	// Simulate an in-flight query by acquiring the segment refcount.
	release := exec.AcquireForTest("busy-1")
	defer release()

	req := httptest.NewRequest(http.MethodDelete, "/api/rehydrated/busy-1", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusConflict {
		t.Fatalf("status: %d body=%s", rr.Code, rr.Body.String())
	}
}
