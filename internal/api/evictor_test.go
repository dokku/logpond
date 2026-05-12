package api

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dokku/logpond/internal/catalog"
	"github.com/dokku/logpond/internal/query"
	"github.com/dokku/logpond/internal/segments"
)

func newEvictorEnv(t *testing.T) (*catalog.Catalog, *query.Executor, string) {
	t.Helper()
	dir := t.TempDir()
	cat, err := catalog.Open(context.Background(), filepath.Join(dir, "catalog.db"), nil)
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	t.Cleanup(func() { _ = cat.Close() })
	return cat, query.NewExecutor(cat, nil, nil), dir
}

func insertRehydratedWithEvict(t *testing.T, cat *catalog.Catalog, dataDir, id string, evict time.Time, hasArchive bool) string {
	t.Helper()
	rehydratedDir := filepath.Join(dataDir, "rehydrated")
	if err := os.MkdirAll(rehydratedDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	parquet := filepath.Join(rehydratedDir, segments.ParquetFilename(id))
	if err := os.WriteFile(parquet, []byte("payload"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	now := time.Now().UTC()
	seg := catalog.Segment{
		ID:         id,
		State:      catalog.StateRehydrated,
		TimeStart:  now.Add(-time.Hour),
		TimeEnd:    now,
		RowCount:   1,
		SizeBytes:  7,
		LocalPath:  sql.NullString{String: parquet, Valid: true},
		EvictAfter: sql.NullTime{Time: evict, Valid: true},
		CreatedAt:  now.Add(-time.Hour),
	}
	if hasArchive {
		seg.S3URL = sql.NullString{String: "s3://bkt/" + id, Valid: true}
	}
	if err := cat.InsertSegment(context.Background(), seg); err != nil {
		t.Fatalf("insert: %v", err)
	}
	return parquet
}

func TestEvictor_EvictsExpiredRow(t *testing.T) {
	cat, exec, dir := newEvictorEnv(t)
	past := time.Now().UTC().Add(-time.Minute)
	parquet := insertRehydratedWithEvict(t, cat, dir, "ev-1", past, true)
	ev := NewEvictor(EvictorOptions{Catalog: cat, Executor: exec})
	n, err := ev.Sweep(context.Background())
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if n != 1 {
		t.Fatalf("evicted: %d", n)
	}
	if _, err := os.Stat(parquet); !os.IsNotExist(err) {
		t.Fatalf("parquet still exists: %v", err)
	}
	seg, _ := cat.GetSegment(context.Background(), "ev-1")
	if seg.State != catalog.StateArchived {
		t.Fatalf("state: %s", seg.State)
	}
}

func TestEvictor_SkipsFutureEvict(t *testing.T) {
	cat, exec, dir := newEvictorEnv(t)
	future := time.Now().UTC().Add(time.Hour)
	parquet := insertRehydratedWithEvict(t, cat, dir, "ev-2", future, true)
	ev := NewEvictor(EvictorOptions{Catalog: cat, Executor: exec})
	n, _ := ev.Sweep(context.Background())
	if n != 0 {
		t.Fatalf("evicted: %d", n)
	}
	if _, err := os.Stat(parquet); err != nil {
		t.Fatalf("parquet should still exist: %v", err)
	}
}

func TestEvictor_SkipsBusy(t *testing.T) {
	cat, exec, dir := newEvictorEnv(t)
	past := time.Now().UTC().Add(-time.Minute)
	parquet := insertRehydratedWithEvict(t, cat, dir, "ev-3", past, true)
	release := exec.AcquireForTest("ev-3")
	defer release()
	ev := NewEvictor(EvictorOptions{Catalog: cat, Executor: exec})
	n, _ := ev.Sweep(context.Background())
	if n != 0 {
		t.Fatalf("evicted: %d", n)
	}
	if _, err := os.Stat(parquet); err != nil {
		t.Fatalf("parquet should still exist: %v", err)
	}
}

func TestEvictor_PersistentNeverEvicted(t *testing.T) {
	cat, exec, dir := newEvictorEnv(t)
	// Insert a row with evict_after=NULL.
	rehydratedDir := filepath.Join(dir, "rehydrated")
	if err := os.MkdirAll(rehydratedDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	parquet := filepath.Join(rehydratedDir, segments.ParquetFilename("ev-4"))
	if err := os.WriteFile(parquet, []byte("payload"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	now := time.Now().UTC()
	if err := cat.InsertSegment(context.Background(), catalog.Segment{
		ID:        "ev-4",
		State:     catalog.StateRehydrated,
		TimeStart: now.Add(-time.Hour),
		TimeEnd:   now,
		RowCount:  1,
		SizeBytes: 7,
		LocalPath: sql.NullString{String: parquet, Valid: true},
		CreatedAt: now,
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	ev := NewEvictor(EvictorOptions{Catalog: cat, Executor: exec})
	n, _ := ev.Sweep(context.Background())
	if n != 0 {
		t.Fatalf("evicted persistent row: %d", n)
	}
	if _, err := os.Stat(parquet); err != nil {
		t.Fatalf("persistent file should remain: %v", err)
	}
}
