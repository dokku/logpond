package catalog

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func tempCatalogPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "catalog.db")
}

func TestOpen_AppliesCoreMigrations(t *testing.T) {
	ctx := context.Background()
	path := tempCatalogPath(t)

	c, err := Open(ctx, path, nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer c.Close()

	applied, err := c.AppliedMigrations(ctx)
	if err != nil {
		t.Fatalf("AppliedMigrations: %v", err)
	}
	if !reflect.DeepEqual(applied, []int{1}) {
		t.Errorf("applied: want [1], got %v", applied)
	}

	expectedTables := []string{"segments", "saved_queries", "jobs", "custom_facets", "migrations"}
	for _, tbl := range expectedTables {
		var name string
		if err := c.DB().QueryRowContext(ctx, `SELECT name FROM sqlite_master WHERE type='table' AND name = ?`, tbl).Scan(&name); err != nil {
			t.Errorf("table %s missing: %v", tbl, err)
		}
	}
}

func TestOpen_IsIdempotent(t *testing.T) {
	ctx := context.Background()
	path := tempCatalogPath(t)

	c1, err := Open(ctx, path, nil)
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	if err := c1.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}

	c2, err := Open(ctx, path, nil)
	if err != nil {
		t.Fatalf("second Open: %v", err)
	}
	defer c2.Close()
	applied, err := c2.AppliedMigrations(ctx)
	if err != nil {
		t.Fatalf("AppliedMigrations: %v", err)
	}
	if len(applied) != 1 {
		t.Errorf("expected 1 migration after re-open, got %v", applied)
	}
}

func TestOpen_AppliesAddedMigrationOnReopen(t *testing.T) {
	ctx := context.Background()
	path := tempCatalogPath(t)

	c1, err := Open(ctx, path, nil)
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	c1.Close()

	withV2 := append(CoreMigrations(), Migration{
		Version: 2,
		Stmts:   []string{`CREATE TABLE test_v2(id INTEGER PRIMARY KEY)`},
	})
	c2, err := Open(ctx, path, withV2)
	if err != nil {
		t.Fatalf("second Open: %v", err)
	}
	defer c2.Close()

	applied, err := c2.AppliedMigrations(ctx)
	if err != nil {
		t.Fatalf("AppliedMigrations: %v", err)
	}
	if !reflect.DeepEqual(applied, []int{1, 2}) {
		t.Errorf("applied: want [1 2], got %v", applied)
	}

	// Re-opening with the same set should be a no-op.
	c3path := path
	c2.Close()
	c3, err := Open(ctx, c3path, withV2)
	if err != nil {
		t.Fatalf("third Open: %v", err)
	}
	defer c3.Close()
	applied, err = c3.AppliedMigrations(ctx)
	if err != nil {
		t.Fatalf("AppliedMigrations: %v", err)
	}
	if !reflect.DeepEqual(applied, []int{1, 2}) {
		t.Errorf("expected migrations not to re-run: %v", applied)
	}
}

func TestOpen_RejectsOutOfOrderMigration(t *testing.T) {
	ctx := context.Background()
	path := tempCatalogPath(t)

	// Apply v1+v2.
	withV2 := append(CoreMigrations(), Migration{
		Version: 2,
		Stmts:   []string{`CREATE TABLE test_v2(id INTEGER PRIMARY KEY)`},
	})
	c1, err := Open(ctx, path, withV2)
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	c1.Close()

	// Now try to add v1.5 — must fail.
	skewed := []Migration{
		CoreMigrations()[0],
		{Version: 1, Stmts: []string{`CREATE TABLE skew(id INTEGER PRIMARY KEY)`}},
	}
	skewed[1].Version = 1 // already applied, this is fine (skipped)

	bad := append(CoreMigrations(), Migration{Version: 2, Stmts: []string{`CREATE TABLE conflict(id INT)`}})
	bad = append(bad, Migration{Version: 1, Stmts: []string{`-- noop`}})
	// Insert a strictly-out-of-order migration: v=1 will be skipped (already applied), v=2 too.
	// To exercise the out-of-order path, add a migration with version=2 that is unseen and another with version=3.
	// More direct: apply CoreMigrations() through Open above (v1+v2 applied). Now pass only [v3 then v2].
	mig := []Migration{
		{Version: 3, Stmts: []string{`CREATE TABLE v3(id INT)`}},
		{Version: 2, Stmts: []string{`CREATE TABLE v2dup(id INT)`}}, // already applied
	}
	if _, err := Open(ctx, path, mig); err != nil {
		// v2 is already applied, so it's skipped; v3 lands cleanly. No error expected.
		t.Fatalf("Open with mixed-order list including already-applied v2 failed: %v", err)
	}

	// Truly out-of-order: register an unseen v1.5 (encoded as v1 — already applied, so skipped) plus an unseen v2 (also skipped).
	// The most reliable way to trigger the error: open a fresh catalog and pass a migration with maxApplied=5 and m.Version=3 unapplied.
	freshPath := filepath.Join(t.TempDir(), "fresh.db")
	preload := []Migration{{Version: 5, Stmts: []string{`CREATE TABLE m5(id INT)`}}}
	c, err := Open(ctx, freshPath, preload)
	if err != nil {
		t.Fatalf("preload: %v", err)
	}
	c.Close()

	conflict := []Migration{
		{Version: 3, Stmts: []string{`CREATE TABLE m3(id INT)`}},
		{Version: 5, Stmts: []string{`CREATE TABLE m5(id INT)`}}, // already applied
	}
	_, err = Open(ctx, freshPath, conflict)
	if !errors.Is(err, ErrMigrationOutOfOrder) {
		t.Fatalf("expected ErrMigrationOutOfOrder, got %v", err)
	}
}

func TestGetSegment_ReturnsInsertedRow(t *testing.T) {
	ctx := context.Background()
	c, err := Open(ctx, tempCatalogPath(t), nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer c.Close()

	_, err = c.DB().ExecContext(ctx, `INSERT INTO segments
        (id, state, time_start, time_end, row_count, size_bytes, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)`,
		"202605120000", "active",
		"2026-05-12T00:00:00Z", "2026-05-12T01:00:00Z",
		42, 1024,
		"2026-05-12T00:00:00Z",
	)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	s, err := c.GetSegment(ctx, "202605120000")
	if err != nil {
		t.Fatalf("GetSegment: %v", err)
	}
	if s.ID != "202605120000" || s.State != "active" || s.RowCount != 42 || s.SizeBytes != 1024 {
		t.Errorf("unexpected segment: %+v", s)
	}

	segs, err := c.ListSegments(ctx)
	if err != nil {
		t.Fatalf("ListSegments: %v", err)
	}
	if len(segs) != 1 || segs[0].ID != "202605120000" {
		t.Errorf("ListSegments: %+v", segs)
	}
}

func TestGetSegment_MissingReturnsErrNoRows(t *testing.T) {
	ctx := context.Background()
	c, err := Open(ctx, tempCatalogPath(t), nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer c.Close()
	if _, err := c.GetSegment(ctx, "nope"); err == nil {
		t.Fatal("expected error for missing segment, got nil")
	}
}

func TestApplyMigrations_FailureRollsBackAndSurfacesStatement(t *testing.T) {
	ctx := context.Background()
	bad := []Migration{{
		Version: 1,
		Stmts: []string{
			"CREATE TABLE good(id INTEGER PRIMARY KEY)",
			"CREATE TABLE good(id INTEGER PRIMARY KEY)\n-- duplicate to force failure",
		},
	}}
	_, err := Open(ctx, tempCatalogPath(t), bad)
	if err == nil {
		t.Fatal("expected duplicate-table error, got nil")
	}
	if !strings.Contains(err.Error(), "CREATE TABLE good") {
		t.Errorf("error should include offending stmt: %v", err)
	}
}

func TestListSegments_EmptyOnFreshCatalog(t *testing.T) {
	ctx := context.Background()
	c, err := Open(ctx, tempCatalogPath(t), nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer c.Close()
	segs, err := c.ListSegments(ctx)
	if err != nil {
		t.Fatalf("ListSegments: %v", err)
	}
	if len(segs) != 0 {
		t.Errorf("expected empty list, got %d", len(segs))
	}
}
