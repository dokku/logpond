package catalog

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"
)

func openTestCatalog(t *testing.T) *Catalog {
	t.Helper()
	cat, err := Open(context.Background(), filepath.Join(t.TempDir(), "catalog.db"), nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = cat.Close() })
	return cat
}

func TestMarkSegmentRehydrated_RecordsLocalPathAndEvictAfter(t *testing.T) {
	cat := openTestCatalog(t)
	now := time.Now().UTC()
	if err := cat.InsertSegment(context.Background(), Segment{
		ID:        "seg-1",
		State:     StateArchived,
		TimeStart: now.Add(-time.Hour),
		TimeEnd:   now,
		RowCount:  10,
		SizeBytes: 100,
		S3URL:     sql.NullString{String: "s3://b/k", Valid: true},
		CreatedAt: now,
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	evict := now.Add(7 * 24 * time.Hour)
	if err := cat.MarkSegmentRehydrated(context.Background(), "seg-1", RehydrateUpdate{
		LocalPath:     "/data/rehydrated/seg-1.parquet",
		RowCount:      20,
		SizeBytes:     200,
		ParquetSHA256: "deadbeef",
		EvictAfter:    &evict,
	}); err != nil {
		t.Fatalf("mark: %v", err)
	}
	seg, err := cat.GetSegment(context.Background(), "seg-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if seg.State != StateRehydrated {
		t.Fatalf("state: %s", seg.State)
	}
	if !seg.LocalPath.Valid || seg.LocalPath.String != "/data/rehydrated/seg-1.parquet" {
		t.Fatalf("local_path: %+v", seg.LocalPath)
	}
	if !seg.EvictAfter.Valid {
		t.Fatalf("evict_after should be set")
	}
	if seg.RowCount != 20 || seg.SizeBytes != 200 {
		t.Fatalf("counts: %+v", seg)
	}
	if !seg.ParquetSHA256.Valid || seg.ParquetSHA256.String != "deadbeef" {
		t.Fatalf("sha: %+v", seg.ParquetSHA256)
	}
}

func TestMarkSegmentRehydrated_PersistentLeavesEvictNull(t *testing.T) {
	cat := openTestCatalog(t)
	now := time.Now().UTC()
	_ = cat.InsertSegment(context.Background(), Segment{
		ID: "seg-2", State: StateArchived, TimeStart: now, TimeEnd: now.Add(time.Hour),
		RowCount: 1, SizeBytes: 1, CreatedAt: now,
	})
	if err := cat.MarkSegmentRehydrated(context.Background(), "seg-2", RehydrateUpdate{
		LocalPath:  "/p",
		EvictAfter: nil,
	}); err != nil {
		t.Fatalf("mark: %v", err)
	}
	seg, _ := cat.GetSegment(context.Background(), "seg-2")
	if seg.EvictAfter.Valid {
		t.Fatalf("evict_after should be NULL for persistent")
	}
}

func TestSetEvictAfter_BumpsTTL(t *testing.T) {
	cat := openTestCatalog(t)
	now := time.Now().UTC()
	original := now.Add(time.Hour)
	_ = cat.InsertSegment(context.Background(), Segment{
		ID: "seg-3", State: StateRehydrated, TimeStart: now.Add(-time.Hour), TimeEnd: now,
		RowCount: 1, SizeBytes: 1, CreatedAt: now,
		EvictAfter: sql.NullTime{Time: original, Valid: true},
	})
	bumped := now.Add(8 * 24 * time.Hour)
	if err := cat.SetEvictAfter(context.Background(), "seg-3", &bumped); err != nil {
		t.Fatalf("set: %v", err)
	}
	seg, _ := cat.GetSegment(context.Background(), "seg-3")
	if !seg.EvictAfter.Time.Equal(bumped) {
		t.Fatalf("evict_after: got %s want %s", seg.EvictAfter.Time, bumped)
	}
}

func TestListRehydratedSegments_OrdersByEvictAfterAscNullsLast(t *testing.T) {
	cat := openTestCatalog(t)
	now := time.Now().UTC()
	mk := func(id string, evict *time.Time) {
		seg := Segment{
			ID: id, State: StateRehydrated, TimeStart: now.Add(-time.Hour), TimeEnd: now,
			RowCount: 1, SizeBytes: 1, CreatedAt: now,
		}
		if evict != nil {
			seg.EvictAfter = sql.NullTime{Time: *evict, Valid: true}
		}
		_ = cat.InsertSegment(context.Background(), seg)
	}
	pastE := now.Add(-time.Minute)
	futureE := now.Add(time.Hour)
	mk("persistent", nil)
	mk("future", &futureE)
	mk("expired", &pastE)
	rows, err := cat.ListRehydratedSegments(context.Background())
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("rows: %d", len(rows))
	}
	if rows[0].ID != "expired" || rows[1].ID != "future" || rows[2].ID != "persistent" {
		t.Fatalf("order: %v", []string{rows[0].ID, rows[1].ID, rows[2].ID})
	}
}

func TestInsertImportedSegment_RoundTrip(t *testing.T) {
	cat := openTestCatalog(t)
	now := time.Now().UTC()
	evict := now.Add(time.Hour)
	if err := cat.InsertImportedSegment(context.Background(), ImportedSegment{
		ID:             "imp-1",
		TimeStart:      now.Add(-time.Hour),
		TimeEnd:        now,
		RowCount:       42,
		SizeBytes:      4242,
		SizeCompressed: 4000,
		LocalPath:      "/data/rehydrated/imp-1.parquet",
		ParquetSHA256:  "abc",
		ManifestSHA256: "def",
		SourceNames:    `["a","b"]`,
		EvictAfter:     &evict,
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	seg, err := cat.GetSegment(context.Background(), "imp-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if seg.State != StateRehydrated {
		t.Fatalf("state: %s", seg.State)
	}
	if !seg.LocalPath.Valid || !seg.ParquetSHA256.Valid || !seg.ManifestSHA256.Valid {
		t.Fatalf("nulls: %+v", seg)
	}
	if !seg.RehydratedAt.Valid {
		t.Fatalf("rehydrated_at should be set")
	}
}
