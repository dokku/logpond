package segments_test

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dokku/logpond/internal/catalog"
	"github.com/dokku/logpond/internal/ingest"
	"github.com/dokku/logpond/internal/storage/duckdb"
)

func TestRecover_RegistersActiveOrphan(t *testing.T) {
	ctx := context.Background()
	clock := &fakeClock{}
	clock.Set(time.Date(2026, 5, 12, 14, 30, 0, 0, time.UTC))
	mgr, cat, dir := newTestManager(t, clock)

	// Manually drop a DuckDB file in the active dir.
	id := "202605121300"
	path := filepath.Join(dir, "segments", "active", id+".duckdb")
	c, err := duckdb.Open(ctx, path, "")
	if err != nil {
		t.Fatalf("seed duckdb: %v", err)
	}
	_ = c.Close()

	rec, err := mgr.Recover(ctx)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if len(rec.ActiveRegistered) != 1 || rec.ActiveRegistered[0] != id {
		t.Errorf("ActiveRegistered = %v, want [%s]", rec.ActiveRegistered, id)
	}
	seg, err := cat.GetSegment(ctx, id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if seg.State != catalog.StateActive {
		t.Errorf("state = %q, want active", seg.State)
	}
}

func TestRecover_RegistersSealedOrphan(t *testing.T) {
	ctx := context.Background()
	clock := &fakeClock{}
	clock.Set(time.Date(2026, 5, 12, 14, 30, 0, 0, time.UTC))
	mgr, cat, dir := newTestManager(t, clock)

	// Build a sealed parquet file via a real DuckDB export.
	id := "202605121300"
	srcPath := filepath.Join(dir, "tmp.duckdb")
	c, err := duckdb.Open(ctx, srcPath, "")
	if err != nil {
		t.Fatalf("seed open: %v", err)
	}
	if err := c.InsertBatch(ctx, []ingest.Event{{
		Timestamp: time.Date(2026, 5, 12, 13, 30, 0, 0, time.UTC),
		Source:    "default", Level: "info", Raw: "{}",
		Attributes: map[string]any{},
	}}); err != nil {
		t.Fatalf("seed insert: %v", err)
	}
	parquetPath := filepath.Join(dir, "segments", "sealed", "segment-"+id+".parquet")
	if err := c.ExportParquet(ctx, parquetPath); err != nil {
		t.Fatalf("seed export: %v", err)
	}
	_ = c.Close()
	_ = os.Remove(srcPath)

	rec, err := mgr.Recover(ctx)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if len(rec.SealedRegistered) != 1 || rec.SealedRegistered[0] != id {
		t.Errorf("SealedRegistered = %v, want [%s]", rec.SealedRegistered, id)
	}
	seg, err := cat.GetSegment(ctx, id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if seg.State != catalog.StateSealed {
		t.Errorf("state = %q, want sealed", seg.State)
	}
	if seg.RowCount != 1 {
		t.Errorf("row_count = %d, want 1", seg.RowCount)
	}
}

func TestRecover_OrphansDuckDBWhenSealedPresent(t *testing.T) {
	ctx := context.Background()
	clock := &fakeClock{}
	clock.Set(time.Date(2026, 5, 12, 14, 30, 0, 0, time.UTC))
	mgr, _, dir := newTestManager(t, clock)

	id := "202605121300"
	activePath := filepath.Join(dir, "segments", "active", id+".duckdb")
	c, err := duckdb.Open(ctx, activePath, "")
	if err != nil {
		t.Fatalf("seed active: %v", err)
	}
	_ = c.Close()

	// Seed a sealed parquet for the same id.
	srcPath := filepath.Join(dir, "tmp.duckdb")
	src, err := duckdb.Open(ctx, srcPath, "")
	if err != nil {
		t.Fatalf("seed src: %v", err)
	}
	if err := src.InsertBatch(ctx, []ingest.Event{{
		Timestamp: time.Date(2026, 5, 12, 13, 30, 0, 0, time.UTC),
		Source:    "default", Level: "info", Raw: "{}",
		Attributes: map[string]any{},
	}}); err != nil {
		t.Fatalf("seed insert: %v", err)
	}
	parquetPath := filepath.Join(dir, "segments", "sealed", "segment-"+id+".parquet")
	if err := src.ExportParquet(ctx, parquetPath); err != nil {
		t.Fatalf("seed export: %v", err)
	}
	_ = src.Close()
	_ = os.Remove(srcPath)

	rec, err := mgr.Recover(ctx)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if len(rec.OrphansMoved) != 1 || rec.OrphansMoved[0] != id {
		t.Errorf("OrphansMoved = %v, want [%s]", rec.OrphansMoved, id)
	}
	if _, err := os.Stat(activePath); !os.IsNotExist(err) {
		t.Errorf("active duckdb should be moved, stat=%v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "orphans", id+".duckdb")); err != nil {
		t.Errorf("orphan duckdb missing: %v", err)
	}
}

func TestRecover_MarksMissingAsLost(t *testing.T) {
	ctx := context.Background()
	clock := &fakeClock{}
	clock.Set(time.Date(2026, 5, 12, 14, 30, 0, 0, time.UTC))
	mgr, cat, _ := newTestManager(t, clock)

	// Pre-insert a catalog row with no backing file.
	id := "202605121300"
	if err := cat.InsertSegment(ctx, catalog.Segment{
		ID:        id,
		State:     catalog.StateSealed,
		TimeStart: time.Date(2026, 5, 12, 13, 0, 0, 0, time.UTC),
		TimeEnd:   time.Date(2026, 5, 12, 14, 0, 0, 0, time.UTC),
		RowCount:  1,
		SizeBytes: 1,
		LocalPath: sql.NullString{String: "/does-not-exist", Valid: true},
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	rec, err := mgr.Recover(ctx)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if len(rec.LostMarked) != 1 || rec.LostMarked[0] != id {
		t.Errorf("LostMarked = %v, want [%s]", rec.LostMarked, id)
	}
	seg, _ := cat.GetSegment(ctx, id)
	if seg.State != catalog.StateLost {
		t.Errorf("state = %q, want lost", seg.State)
	}
}
