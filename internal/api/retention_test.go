package api

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dokku/logpond/internal/catalog"
	"github.com/dokku/logpond/internal/ingest"
	"github.com/dokku/logpond/internal/metrics"
	"github.com/dokku/logpond/internal/retention"
)

func newRetentionTestServer(t *testing.T) (*Server, *catalog.Catalog) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "catalog.db")
	cat, err := catalog.Open(context.Background(), path, nil)
	if err != nil {
		t.Fatalf("open catalog: %v", err)
	}
	t.Cleanup(func() { _ = cat.Close() })

	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	eval, err := retention.New(retention.Options{
		Catalog: cat,
		MaxAge:  24 * time.Hour,
		Now:     func() time.Time { return time.Date(2026, 5, 12, 12, 0, 0, 0, time.UTC) },
		Logger:  logger,
	})
	if err != nil {
		t.Fatalf("retention.New: %v", err)
	}

	buf := ingest.NewBuffer(8)
	m := metrics.New(metrics.Options{FillRatio: buf.FillRatio})
	srv := New(Options{
		Logger:    logger,
		Buffer:    buf,
		Metrics:   m,
		Retention: eval,
	})
	return srv, cat
}

func TestRetentionRun_DryRunReportsActionsWithoutMutation(t *testing.T) {
	srv, cat := newRetentionTestServer(t)
	now := time.Date(2026, 5, 12, 12, 0, 0, 0, time.UTC)

	dir := t.TempDir()
	file := filepath.Join(dir, "segment.parquet")
	if err := os.WriteFile(file, []byte("data"), 0o644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	start := now.Add(-72 * time.Hour)
	if err := cat.InsertSegment(context.Background(), catalog.Segment{
		ID:        "old",
		State:     catalog.StateSealed,
		TimeStart: start,
		TimeEnd:   start.Add(time.Hour),
		RowCount:  10,
		SizeBytes: 1024,
		LocalPath: sql.NullString{String: file, Valid: true},
		SealedAt:  sql.NullTime{Time: start.Add(time.Hour), Valid: true},
		CreatedAt: start,
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/admin/retention/run",
		bytes.NewBufferString(`{"dry_run":true}`))
	req.Header.Set("Content-Type", "application/json")
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status: %d body=%s", rr.Code, rr.Body)
	}
	var out retention.Result
	if err := json.NewDecoder(rr.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !out.DryRun || len(out.Actions) != 1 {
		t.Fatalf("unexpected result: %+v", out)
	}
	if _, err := os.Stat(file); err != nil {
		t.Errorf("file removed in dry run: %v", err)
	}
}

func TestRetentionRun_EmptyBodyDefaultsToExecute(t *testing.T) {
	srv, _ := newRetentionTestServer(t)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/admin/retention/run", strings.NewReader(""))
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status: %d body=%s", rr.Code, rr.Body)
	}
	var out retention.Result
	if err := json.NewDecoder(rr.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.DryRun {
		t.Errorf("expected dry_run=false on empty body, got true")
	}
}

func TestRetentionRun_ServiceUnavailableWhenDisabled(t *testing.T) {
	buf := ingest.NewBuffer(8)
	m := metrics.New(metrics.Options{FillRatio: buf.FillRatio})
	srv := New(Options{Buffer: buf, Metrics: m})
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/admin/retention/run", nil)
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status: %d", rr.Code)
	}
	mustErrorCode(t, rr, "retention_unavailable")
}
