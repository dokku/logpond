package query_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/dokku/logpond/internal/catalog"
	"github.com/dokku/logpond/internal/ingest"
	"github.com/dokku/logpond/internal/query"
	"github.com/dokku/logpond/internal/segments"
)

func newFixture(t *testing.T) (*query.Executor, *segments.Manager, *catalog.Catalog, func(time.Time)) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	cat, err := catalog.Open(ctx, filepath.Join(dir, "catalog.db"), nil)
	if err != nil {
		t.Fatalf("open catalog: %v", err)
	}
	t.Cleanup(func() { _ = cat.Close() })

	now := time.Date(2026, 5, 12, 14, 30, 0, 0, time.UTC)
	var clock time.Time = now
	mgr, err := segments.New(segments.Options{
		DataDir:     dir,
		Window:      time.Hour,
		SealGrace:   time.Second,
		MemoryLimit: "128MB",
		Catalog:     cat,
		Now:         func() time.Time { return clock },
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Close() })

	exec := query.NewExecutor(cat, mgr, nil)
	advance := func(t time.Time) { clock = t.UTC() }
	return exec, mgr, cat, advance
}

func TestExecutor_QueriesSealedSegment(t *testing.T) {
	ctx := context.Background()
	exec, mgr, _, advance := newFixture(t)

	advance(time.Date(2026, 5, 12, 13, 30, 0, 0, time.UTC))
	events := []ingest.Event{
		mkEvent(t, "2026-05-12T13:30:00Z", "api", "info", "alpha hello", map[string]any{"user_id": 42}),
		mkEvent(t, "2026-05-12T13:30:05Z", "api", "error", "beta refused", map[string]any{"user_id": 99}),
		mkEvent(t, "2026-05-12T13:30:10Z", "worker", "info", "gamma starting", map[string]any{"user_id": 42}),
	}
	if err := mgr.Flush(ctx, events); err != nil {
		t.Fatalf("flush: %v", err)
	}
	// Advance and seal.
	advance(time.Date(2026, 5, 12, 14, 5, 0, 0, time.UTC))
	if _, err := mgr.SealOnce(ctx); err != nil {
		t.Fatalf("seal: %v", err)
	}

	resp, err := exec.Run(ctx, query.Request{
		From: time.Date(2026, 5, 12, 13, 0, 0, 0, time.UTC),
		To:   time.Date(2026, 5, 12, 14, 0, 0, 0, time.UTC),
		Filter: query.Leaf{
			Field: "service", Op: query.OpEq, Value: "api", CaseSensitive: true,
		},
		MaxTimeRange: 24 * time.Hour,
	})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := len(resp.Events); got != 2 {
		t.Fatalf("events = %d, want 2", got)
	}
	if resp.Events[0].Timestamp.Before(resp.Events[1].Timestamp) {
		t.Errorf("default sort should be timestamp desc")
	}
	if resp.Stats.SegmentsRead != 1 {
		t.Errorf("segments_read = %d, want 1", resp.Stats.SegmentsRead)
	}
}

func TestExecutor_QueriesActiveSegment(t *testing.T) {
	ctx := context.Background()
	exec, mgr, _, advance := newFixture(t)

	advance(time.Date(2026, 5, 12, 14, 30, 0, 0, time.UTC))
	events := []ingest.Event{
		mkEvent(t, "2026-05-12T14:30:00Z", "api", "info", "fresh", map[string]any{}),
		mkEvent(t, "2026-05-12T14:30:05Z", "api", "error", "bang", map[string]any{}),
	}
	if err := mgr.Flush(ctx, events); err != nil {
		t.Fatalf("flush: %v", err)
	}
	resp, err := exec.Run(ctx, query.Request{
		From:         time.Date(2026, 5, 12, 14, 0, 0, 0, time.UTC),
		To:           time.Date(2026, 5, 12, 15, 0, 0, 0, time.UTC),
		Filter:       query.Leaf{Field: "level", Op: query.OpEq, Value: "error", CaseSensitive: true},
		MaxTimeRange: 24 * time.Hour,
	})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(resp.Events) != 1 {
		t.Fatalf("events = %d, want 1", len(resp.Events))
	}
	if resp.Events[0].Message.String != "bang" {
		t.Errorf("got %q, want bang", resp.Events[0].Message.String)
	}
}

func TestExecutor_FreeTextSearchAcrossMessageAndRaw(t *testing.T) {
	ctx := context.Background()
	exec, mgr, _, advance := newFixture(t)
	advance(time.Date(2026, 5, 12, 14, 30, 0, 0, time.UTC))
	if err := mgr.Flush(ctx, []ingest.Event{
		mkEvent(t, "2026-05-12T14:30:00Z", "api", "info", "Connection refused", nil),
		mkEvent(t, "2026-05-12T14:30:01Z", "api", "info", "All good", nil),
	}); err != nil {
		t.Fatalf("flush: %v", err)
	}
	resp, err := exec.Run(ctx, query.Request{
		From:         time.Date(2026, 5, 12, 14, 0, 0, 0, time.UTC),
		To:           time.Date(2026, 5, 12, 15, 0, 0, 0, time.UTC),
		Search:       "REFUSED",
		MaxTimeRange: 24 * time.Hour,
	})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(resp.Events) != 1 {
		t.Fatalf("got %d events, want 1", len(resp.Events))
	}
}

func TestExecutor_PaginationCursor(t *testing.T) {
	ctx := context.Background()
	exec, mgr, _, advance := newFixture(t)

	advance(time.Date(2026, 5, 12, 14, 30, 0, 0, time.UTC))
	base := time.Date(2026, 5, 12, 14, 30, 0, 0, time.UTC)
	const total = 250
	batch := make([]ingest.Event, 0, total)
	for i := 0; i < total; i++ {
		batch = append(batch, mkEvent(t, base.Add(time.Duration(i)*time.Millisecond).Format(time.RFC3339Nano),
			"api", "info", fmt.Sprintf("msg-%d", i), nil))
	}
	if err := mgr.Flush(ctx, batch); err != nil {
		t.Fatalf("flush: %v", err)
	}

	seen := map[string]bool{}
	cursor := ""
	for page := 0; page < 4; page++ {
		resp, err := exec.Run(ctx, query.Request{
			From:         time.Date(2026, 5, 12, 14, 0, 0, 0, time.UTC),
			To:           time.Date(2026, 5, 12, 15, 0, 0, 0, time.UTC),
			Limit:        100,
			Cursor:       cursor,
			MaxTimeRange: 24 * time.Hour,
		})
		if err != nil {
			t.Fatalf("page %d: %v", page, err)
		}
		for _, ev := range resp.Events {
			ts := ev.Timestamp.Format(time.RFC3339Nano)
			if seen[ts] {
				t.Errorf("page %d: duplicate ts %s", page, ts)
			}
			seen[ts] = true
		}
		if resp.NextCursor == "" {
			break
		}
		cursor = resp.NextCursor
	}
	if len(seen) != total {
		t.Errorf("paged through %d events, want %d", len(seen), total)
	}
}

func TestExecutor_TimeRangeTooLarge(t *testing.T) {
	ctx := context.Background()
	exec, _, _, _ := newFixture(t)
	_, err := exec.Run(ctx, query.Request{
		From:         time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC),
		To:           time.Date(2026, 5, 12, 0, 0, 0, 0, time.UTC),
		MaxTimeRange: 7 * 24 * time.Hour,
	})
	if err == nil || err != query.ErrTimeRangeTooLarge {
		t.Errorf("err = %v, want ErrTimeRangeTooLarge", err)
	}
}

func TestExecutor_FilterTooDeep(t *testing.T) {
	ctx := context.Background()
	exec, _, _, _ := newFixture(t)
	var node query.Node = query.Leaf{Field: "service", Op: query.OpEq, Value: "x"}
	for i := 0; i < 33; i++ {
		node = query.Group{Op: query.OpAnd, Children: []query.Node{node}}
	}
	_, err := exec.Run(ctx, query.Request{
		From:         time.Date(2026, 5, 12, 0, 0, 0, 0, time.UTC),
		To:           time.Date(2026, 5, 12, 1, 0, 0, 0, time.UTC),
		Filter:       node,
		MaxTimeRange: 24 * time.Hour,
	})
	if !query.IsFilterTooDeep(err) {
		t.Errorf("err = %v, want filter_too_deep", err)
	}
}

func mkEvent(t *testing.T, ts, service, level, message string, attrs map[string]any) ingest.Event {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339Nano, ts)
	if err != nil {
		t.Fatalf("parse ts %q: %v", ts, err)
	}
	if attrs == nil {
		attrs = map[string]any{}
	}
	return ingest.Event{
		Timestamp:  parsed.UTC(),
		Service:    service,
		Level:      level,
		Message:    message,
		Source:     "default",
		Attributes: attrs,
		Raw:        fmt.Sprintf(`{"timestamp":%q,"service":%q,"level":%q,"msg":%q}`, ts, service, level, message),
	}
}
