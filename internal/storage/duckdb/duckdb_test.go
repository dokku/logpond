package duckdb_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/dokku/logpond/internal/ingest"
	"github.com/dokku/logpond/internal/storage/duckdb"
)

func TestConnection_InsertAndSelect(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "seg.duckdb")
	c, err := duckdb.Open(ctx, path, "128MB")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer c.Close()

	events := []ingest.Event{
		{
			Timestamp: time.Date(2026, 5, 12, 14, 0, 0, 0, time.UTC),
			Service:   "api",
			Level:     "info",
			Message:   "hello",
			Host:      "h1",
			Source:    "default",
			Attributes: map[string]any{
				"user_id":      42,
				"logpond_late": false,
			},
			Raw: `{"msg":"hello"}`,
		},
		{
			Timestamp:  time.Date(2026, 5, 12, 14, 0, 1, 0, time.UTC),
			Level:      "error",
			Source:     "default",
			Attributes: map[string]any{},
			Raw:        `{"msg":"oops"}`,
		},
	}
	if err := c.InsertBatch(ctx, events); err != nil {
		t.Fatalf("insert: %v", err)
	}

	n, err := c.RowCount(ctx)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 2 {
		t.Fatalf("rows = %d, want 2", n)
	}

	rows, err := c.DB().QueryContext(ctx, "SELECT service, level, attributes FROM events ORDER BY timestamp")
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	defer rows.Close()

	var got []struct {
		Service sql.NullString
		Level   sql.NullString
		Attrs   string
	}
	for rows.Next() {
		var r struct {
			Service sql.NullString
			Level   sql.NullString
			Attrs   string
		}
		if err := rows.Scan(&r.Service, &r.Level, &r.Attrs); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, r)
	}
	if len(got) != 2 {
		t.Fatalf("rows scanned = %d, want 2", len(got))
	}
	if got[0].Service.String != "api" || got[0].Level.String != "info" {
		t.Errorf("row 0 mismatch: %+v", got[0])
	}
	if got[1].Service.Valid {
		t.Errorf("row 1 should have NULL service, got %q", got[1].Service.String)
	}
}

func TestConnection_ExportParquet(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "src.duckdb")
	c, err := duckdb.Open(ctx, srcPath, "")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer c.Close()

	now := time.Date(2026, 5, 12, 14, 0, 0, 0, time.UTC)
	events := make([]ingest.Event, 0, 5)
	for i := 0; i < 5; i++ {
		events = append(events, ingest.Event{
			Timestamp:  now.Add(time.Duration(i) * time.Second),
			Source:     "default",
			Level:      "info",
			Message:    "row",
			Attributes: map[string]any{"i": i},
			Raw:        `{}`,
		})
	}
	if err := c.InsertBatch(ctx, events); err != nil {
		t.Fatalf("insert: %v", err)
	}

	parquetPath := filepath.Join(dir, "out.parquet")
	if err := c.ExportParquet(ctx, parquetPath); err != nil {
		t.Fatalf("export: %v", err)
	}

	// Read the Parquet file back via a fresh in-memory DuckDB.
	verify, err := duckdb.Open(ctx, "", "")
	if err != nil {
		t.Fatalf("verify open: %v", err)
	}
	defer verify.Close()
	var rows int64
	if err := verify.DB().QueryRowContext(ctx,
		"SELECT count(*) FROM read_parquet('"+parquetPath+"')").Scan(&rows); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if rows != 5 {
		t.Errorf("read_parquet rows = %d, want 5", rows)
	}
}
