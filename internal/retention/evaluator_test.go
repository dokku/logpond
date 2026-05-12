package retention

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dokku/logpond/internal/archive"
	"github.com/dokku/logpond/internal/catalog"
)

// fakeBackend captures the calls the evaluator makes; it always
// succeeds and reports an s3:// URL derived from the segment ref so
// MarkSegmentArchived has something concrete to write.
type fakeBackend struct {
	calls    []string
	failNext bool
}

func (f *fakeBackend) Capabilities() archive.Capabilities {
	return archive.Capabilities{Archive: true, Retrieve: true, Verify: true, Name: "fake"}
}

func (f *fakeBackend) CapabilityDetail() archive.CapabilityDetail {
	return archive.CapabilityDetail{Backend: "fake", Archive: "yes", Retrieve: "yes", Verify: "yes"}
}

func (f *fakeBackend) Archive(_ context.Context, ref archive.SegmentRef) (archive.ArchiveResult, error) {
	f.calls = append(f.calls, ref.ID)
	if f.failNext {
		f.failNext = false
		return archive.ArchiveResult{}, errors.New("injected archive failure")
	}
	return archive.ArchiveResult{
		S3URL:          "s3://bucket/segments/" + ref.ID + ".parquet",
		ParquetSHA256:  ref.ParquetSHA256,
		ManifestSHA256: "manifest-sha-" + ref.ID,
	}, nil
}

func (f *fakeBackend) Retrieve(context.Context, archive.SegmentRef, string) (archive.RetrieveResult, error) {
	return archive.RetrieveResult{}, archive.ErrUnsupported
}

func (f *fakeBackend) Verify(context.Context, []string) (archive.VerifyResult, error) {
	return archive.VerifyResult{Backend: "fake"}, nil
}

// fixedNow returns a clock that always reports t.
func fixedNow(t time.Time) func() time.Time { return func() time.Time { return t } }

// silentLogger returns an slog.Logger that discards everything so test
// runs don't fill the buffer with retention chatter.
func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

// openCatalog creates a temp catalog ready for inserts.
func openCatalog(t *testing.T) *catalog.Catalog {
	t.Helper()
	path := filepath.Join(t.TempDir(), "catalog.db")
	c, err := catalog.Open(context.Background(), path, nil)
	if err != nil {
		t.Fatalf("open catalog: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// segOpts is the bag of fields a test fixture can override on the
// default seal/rehydrate row.
type segOpts struct {
	state      string
	sealedAt   time.Time
	hasArchive bool
	localPath  string
	sizeBytes  int64
}

// insertSegment writes a row to the catalog. `start` is the segment's
// window start; the window end is start+1h.
func insertSegment(t *testing.T, c *catalog.Catalog, id string, start time.Time, opts segOpts) {
	t.Helper()
	if opts.state == "" {
		opts.state = catalog.StateSealed
	}
	if opts.sealedAt.IsZero() {
		opts.sealedAt = start.Add(time.Hour)
	}
	if opts.sizeBytes == 0 {
		opts.sizeBytes = 1024
	}
	s := catalog.Segment{
		ID:        id,
		State:     opts.state,
		TimeStart: start,
		TimeEnd:   start.Add(time.Hour),
		RowCount:  10,
		SizeBytes: opts.sizeBytes,
		LocalPath: sql.NullString{String: opts.localPath, Valid: opts.localPath != ""},
		SealedAt:  sql.NullTime{Time: opts.sealedAt, Valid: !opts.sealedAt.IsZero()},
		CreatedAt: start,
	}
	if opts.hasArchive {
		s.S3URL = sql.NullString{String: "s3://bucket/" + id, Valid: true}
		s.ArchivedAt = sql.NullTime{Time: opts.sealedAt.Add(time.Minute), Valid: true}
	}
	if err := c.InsertSegment(context.Background(), s); err != nil {
		t.Fatalf("insert %s: %v", id, err)
	}
}

func TestEvaluator_MaxAge_FlagsOldSegments(t *testing.T) {
	now := time.Date(2026, 5, 12, 12, 0, 0, 0, time.UTC)
	c := openCatalog(t)

	// Five sealed segments spanning the last few days. Window start 9h-72h ago.
	insertSegment(t, c, "old1", now.Add(-72*time.Hour), segOpts{localPath: "/tmp/old1"})
	insertSegment(t, c, "old2", now.Add(-48*time.Hour), segOpts{localPath: "/tmp/old2"})
	insertSegment(t, c, "mid", now.Add(-26*time.Hour), segOpts{localPath: "/tmp/mid"})
	insertSegment(t, c, "fresh", now.Add(-9*time.Hour), segOpts{localPath: "/tmp/fresh"})

	e, err := New(Options{
		Catalog: c,
		MaxAge:  24 * time.Hour,
		Now:     fixedNow(now),
		Logger:  silentLogger(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	r, err := e.Plan(context.Background())
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if r.Evaluated != 4 {
		t.Errorf("evaluated: want 4, got %d", r.Evaluated)
	}
	got := map[string]string{}
	for _, a := range r.Actions {
		got[a.SegmentID] = a.Action
	}
	wantSelected := []string{"old1", "old2", "mid"}
	for _, id := range wantSelected {
		if _, ok := got[id]; !ok {
			t.Errorf("expected %s to be selected, actions: %+v", id, got)
		}
	}
	if _, ok := got["fresh"]; ok {
		t.Errorf("expected fresh to be skipped, actions: %+v", got)
	}
}

func TestEvaluator_ProtectedWindow_SkipsRecentSeals(t *testing.T) {
	now := time.Date(2026, 5, 12, 12, 0, 0, 0, time.UTC)
	c := openCatalog(t)

	// Segment window is old, but sealed_at is within the 1h protected
	// window — must not be deleted.
	insertSegment(t, c, "recent-seal", now.Add(-72*time.Hour),
		segOpts{localPath: "/tmp/recent", sealedAt: now.Add(-30 * time.Minute)})

	e, err := New(Options{
		Catalog: c,
		MaxAge:  24 * time.Hour,
		Now:     fixedNow(now),
		Logger:  silentLogger(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	r, err := e.Plan(context.Background())
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if len(r.Actions) != 0 {
		t.Errorf("expected no actions, got %+v", r.Actions)
	}
}

func TestEvaluator_MaxSize_OldestFirstUntilUnderCap(t *testing.T) {
	now := time.Date(2026, 5, 12, 12, 0, 0, 0, time.UTC)
	c := openCatalog(t)

	// Three segments of 5GB each, total 15GB. Cap is 10GB → expect the
	// oldest one to be evicted (size 5GB removed leaves 10GB, which is
	// exactly the cap; the loop exits with over <= 0).
	gb := int64(1 << 30)
	insertSegment(t, c, "s1", now.Add(-10*time.Hour), segOpts{localPath: "/tmp/s1", sizeBytes: 5 * gb})
	insertSegment(t, c, "s2", now.Add(-9*time.Hour), segOpts{localPath: "/tmp/s2", sizeBytes: 5 * gb})
	insertSegment(t, c, "s3", now.Add(-8*time.Hour), segOpts{localPath: "/tmp/s3", sizeBytes: 5 * gb})

	e, err := New(Options{
		Catalog: c,
		MaxSize: 10 * gb,
		Now:     fixedNow(now),
		Logger:  silentLogger(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	r, err := e.Plan(context.Background())
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if len(r.Actions) != 1 || r.Actions[0].SegmentID != "s1" {
		t.Fatalf("expected only oldest s1 selected, got %+v", r.Actions)
	}
}

func TestEvaluator_ArchiveBeforeDelete_StaysPending(t *testing.T) {
	now := time.Date(2026, 5, 12, 12, 0, 0, 0, time.UTC)
	c := openCatalog(t)
	insertSegment(t, c, "old", now.Add(-72*time.Hour), segOpts{localPath: "/tmp/old"})

	e, err := New(Options{
		Catalog:             c,
		MaxAge:              24 * time.Hour,
		ArchiveBeforeDelete: true,
		BackendActive:       true,
		Now:                 fixedNow(now),
		Logger:              silentLogger(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	r, err := e.Run(context.Background(), false)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(r.Actions) != 1 || r.Actions[0].Action != ActionArchiveThenDelete {
		t.Fatalf("expected archive_then_delete, got %+v", r.Actions)
	}
	if r.Actions[0].Executed {
		t.Errorf("archive_then_delete must not execute in Phase 7")
	}
	// Catalog row should remain untouched.
	s, err := c.GetSegment(context.Background(), "old")
	if err != nil {
		t.Fatalf("GetSegment: %v", err)
	}
	if s.State != catalog.StateSealed || !s.LocalPath.Valid {
		t.Errorf("segment mutated unexpectedly: %+v", s)
	}
}

func TestEvaluator_Run_DeletesLocalFileAndUpdatesCatalog(t *testing.T) {
	now := time.Date(2026, 5, 12, 12, 0, 0, 0, time.UTC)
	c := openCatalog(t)

	dir := t.TempDir()
	file := filepath.Join(dir, "segment.parquet")
	if err := os.WriteFile(file, []byte("data"), 0o644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	insertSegment(t, c, "old", now.Add(-72*time.Hour), segOpts{localPath: file})

	e, err := New(Options{
		Catalog:             c,
		MaxAge:              24 * time.Hour,
		ArchiveBeforeDelete: false,
		BackendActive:       false,
		Now:                 fixedNow(now),
		Logger:              silentLogger(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	r, err := e.Run(context.Background(), false)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(r.Actions) != 1 || !r.Actions[0].Executed {
		t.Fatalf("expected one executed delete, got %+v", r.Actions)
	}
	if _, err := os.Stat(file); !os.IsNotExist(err) {
		t.Errorf("expected file removed, stat err=%v", err)
	}
	s, err := c.GetSegment(context.Background(), "old")
	if err != nil {
		t.Fatalf("GetSegment: %v", err)
	}
	if s.State != catalog.StateLost || s.LocalPath.Valid {
		t.Errorf("expected state=lost local_path cleared, got %+v", s)
	}
}

func TestEvaluator_DryRun_ReportsButDoesNotMutate(t *testing.T) {
	now := time.Date(2026, 5, 12, 12, 0, 0, 0, time.UTC)
	c := openCatalog(t)

	dir := t.TempDir()
	file := filepath.Join(dir, "segment.parquet")
	if err := os.WriteFile(file, []byte("data"), 0o644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	insertSegment(t, c, "old", now.Add(-72*time.Hour), segOpts{localPath: file})

	e, err := New(Options{
		Catalog: c,
		MaxAge:  24 * time.Hour,
		Now:     fixedNow(now),
		Logger:  silentLogger(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	r, err := e.Run(context.Background(), true)
	if err != nil {
		t.Fatalf("Run dry: %v", err)
	}
	if len(r.Actions) != 1 || r.Actions[0].Executed {
		t.Fatalf("dry run must not execute: %+v", r.Actions)
	}
	if _, err := os.Stat(file); err != nil {
		t.Errorf("file should still exist, stat err=%v", err)
	}
	s, err := c.GetSegment(context.Background(), "old")
	if err != nil {
		t.Fatalf("GetSegment: %v", err)
	}
	if s.State != catalog.StateSealed {
		t.Errorf("segment state mutated in dry run: %s", s.State)
	}
}

func TestEvaluator_ArchivedSegment_DeletesFileButKeepsArchive(t *testing.T) {
	now := time.Date(2026, 5, 12, 12, 0, 0, 0, time.UTC)
	c := openCatalog(t)

	dir := t.TempDir()
	file := filepath.Join(dir, "segment.parquet")
	if err := os.WriteFile(file, []byte("data"), 0o644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	insertSegment(t, c, "old", now.Add(-72*time.Hour), segOpts{
		state:      catalog.StateArchived,
		hasArchive: true,
		localPath:  file,
	})

	e, err := New(Options{
		Catalog:             c,
		MaxAge:              24 * time.Hour,
		ArchiveBeforeDelete: true,
		BackendActive:       true,
		Now:                 fixedNow(now),
		Logger:              silentLogger(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	r, err := e.Run(context.Background(), false)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(r.Actions) != 1 || r.Actions[0].Action != ActionDelete || !r.Actions[0].Executed {
		t.Fatalf("expected single executed delete, got %+v", r.Actions)
	}
	s, err := c.GetSegment(context.Background(), "old")
	if err != nil {
		t.Fatalf("GetSegment: %v", err)
	}
	if s.State != catalog.StateArchived {
		t.Errorf("archived segment should remain archived after local file removal: %s", s.State)
	}
	if s.LocalPath.Valid {
		t.Errorf("local_path should be cleared, got %v", s.LocalPath)
	}
	if !s.S3URL.Valid {
		t.Errorf("s3_url should be retained, got %+v", s.S3URL)
	}
}

func TestEvaluator_ArchiveBeforeDelete_RunsBackendThenDeletes(t *testing.T) {
	now := time.Date(2026, 5, 12, 12, 0, 0, 0, time.UTC)
	c := openCatalog(t)

	dir := t.TempDir()
	file := filepath.Join(dir, "segment.parquet")
	if err := os.WriteFile(file, []byte("data"), 0o644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	insertSegment(t, c, "old", now.Add(-72*time.Hour), segOpts{localPath: file})

	backend := &fakeBackend{}
	e, err := New(Options{
		Catalog:             c,
		MaxAge:              24 * time.Hour,
		ArchiveBeforeDelete: true,
		BackendActive:       true,
		Backend:             backend,
		Now:                 fixedNow(now),
		Logger:              silentLogger(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	r, err := e.Run(context.Background(), false)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(r.Actions) != 1 {
		t.Fatalf("expected one action, got %+v", r.Actions)
	}
	a := r.Actions[0]
	if a.Action != ActionArchiveThenDelete {
		t.Fatalf("expected archive_then_delete verb, got %s", a.Action)
	}
	if !a.Executed {
		t.Fatalf("expected action to execute with backend wired, action=%+v", a)
	}
	if len(backend.calls) != 1 || backend.calls[0] != "old" {
		t.Fatalf("expected backend to receive segment old, got %v", backend.calls)
	}
	// Local file gone, catalog state archived with s3_url populated.
	if _, err := os.Stat(file); !os.IsNotExist(err) {
		t.Errorf("expected local file removed, stat err=%v", err)
	}
	s, err := c.GetSegment(context.Background(), "old")
	if err != nil {
		t.Fatalf("GetSegment: %v", err)
	}
	if s.State != catalog.StateArchived {
		t.Errorf("expected archived state, got %s", s.State)
	}
	if !s.S3URL.Valid || s.S3URL.String == "" {
		t.Errorf("expected s3_url populated, got %+v", s.S3URL)
	}
	if s.LocalPath.Valid {
		t.Errorf("expected local_path cleared, got %+v", s.LocalPath)
	}
}

func TestEvaluator_ArchiveFailure_LeavesSegmentSealed(t *testing.T) {
	now := time.Date(2026, 5, 12, 12, 0, 0, 0, time.UTC)
	c := openCatalog(t)

	dir := t.TempDir()
	file := filepath.Join(dir, "segment.parquet")
	if err := os.WriteFile(file, []byte("data"), 0o644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	insertSegment(t, c, "old", now.Add(-72*time.Hour), segOpts{localPath: file})

	backend := &fakeBackend{failNext: true}
	e, err := New(Options{
		Catalog:             c,
		MaxAge:              24 * time.Hour,
		ArchiveBeforeDelete: true,
		BackendActive:       true,
		Backend:             backend,
		Now:                 fixedNow(now),
		Logger:              silentLogger(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	r, err := e.Run(context.Background(), false)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(r.Actions) != 1 || r.Actions[0].Executed {
		t.Fatalf("expected unexecuted action, got %+v", r.Actions)
	}
	if r.Actions[0].Error == "" {
		t.Errorf("expected error to be captured on the action")
	}
	if _, err := os.Stat(file); err != nil {
		t.Errorf("expected file to remain on archive failure, stat err=%v", err)
	}
	s, err := c.GetSegment(context.Background(), "old")
	if err != nil {
		t.Fatalf("GetSegment: %v", err)
	}
	if s.State != catalog.StateSealed {
		t.Errorf("expected segment to remain sealed, got %s", s.State)
	}
}

func TestEvaluator_Configured_FalseWhenBothAxesUnset(t *testing.T) {
	c := openCatalog(t)
	e, err := New(Options{Catalog: c, Logger: silentLogger()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if e.Configured() {
		t.Errorf("expected unconfigured evaluator")
	}
	r, err := e.Plan(context.Background())
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if len(r.Actions) != 0 {
		t.Errorf("expected no actions when unconfigured: %+v", r.Actions)
	}
}
