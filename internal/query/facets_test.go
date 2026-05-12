package query_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/dokku/logpond/internal/ingest"
	"github.com/dokku/logpond/internal/query"
)

func TestExecutor_FacetCounts_AcrossServiceAndLevel(t *testing.T) {
	ctx := context.Background()
	exec, mgr, _, advance := newFixture(t)

	advance(time.Date(2026, 5, 12, 14, 30, 0, 0, time.UTC))
	events := []ingest.Event{
		mkEvent(t, "2026-05-12T14:30:00Z", "api", "info", "a", nil),
		mkEvent(t, "2026-05-12T14:30:01Z", "api", "info", "b", nil),
		mkEvent(t, "2026-05-12T14:30:02Z", "api", "error", "c", nil),
		mkEvent(t, "2026-05-12T14:30:03Z", "worker", "info", "d", nil),
	}
	if err := mgr.Flush(ctx, events); err != nil {
		t.Fatalf("flush: %v", err)
	}

	resp, err := exec.Run(ctx, query.Request{
		From:         time.Date(2026, 5, 12, 14, 0, 0, 0, time.UTC),
		To:           time.Date(2026, 5, 12, 15, 0, 0, 0, time.UTC),
		MaxTimeRange: 24 * time.Hour,
		Facets: []query.FacetSpec{
			{Name: "service", Field: "service", CardinalityCap: 100},
			{Name: "level", Field: "level", CardinalityCap: 10},
		},
	})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	svc := resp.Facets["service"]
	wantSvc := map[string]int64{"api": 3, "worker": 1}
	if got := facetMap(svc.Values); !mapsEqual(got, wantSvc) {
		t.Errorf("service facet = %v, want %v", got, wantSvc)
	}
	lvl := resp.Facets["level"]
	wantLvl := map[string]int64{"info": 3, "error": 1}
	if got := facetMap(lvl.Values); !mapsEqual(got, wantLvl) {
		t.Errorf("level facet = %v, want %v", got, wantLvl)
	}
	if svc.SampleSegments < 1 {
		t.Errorf("service sample_segments = %d, want >=1", svc.SampleSegments)
	}
}

func TestExecutor_FacetTruncation(t *testing.T) {
	ctx := context.Background()
	exec, mgr, _, advance := newFixture(t)
	advance(time.Date(2026, 5, 12, 14, 30, 0, 0, time.UTC))

	var events []ingest.Event
	const total = 20
	for i := 0; i < total; i++ {
		events = append(events, mkEvent(t,
			time.Date(2026, 5, 12, 14, 30, i, 0, time.UTC).Format(time.RFC3339Nano),
			fmt.Sprintf("svc-%02d", i),
			"info",
			"hi",
			nil,
		))
	}
	if err := mgr.Flush(ctx, events); err != nil {
		t.Fatalf("flush: %v", err)
	}

	resp, err := exec.Run(ctx, query.Request{
		From:         time.Date(2026, 5, 12, 14, 0, 0, 0, time.UTC),
		To:           time.Date(2026, 5, 12, 15, 0, 0, 0, time.UTC),
		MaxTimeRange: 24 * time.Hour,
		Facets:       []query.FacetSpec{{Name: "service", Field: "service", CardinalityCap: 5}},
	})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	svc := resp.Facets["service"]
	if len(svc.Values) != 5 {
		t.Errorf("len(values) = %d, want 5", len(svc.Values))
	}
	if !svc.Truncated {
		t.Error("Truncated should be true")
	}
	if svc.TruncatedCount != total {
		t.Errorf("TruncatedCount = %d, want %d", svc.TruncatedCount, total)
	}
}

func TestExecutor_FacetOnAttribute(t *testing.T) {
	ctx := context.Background()
	exec, mgr, _, advance := newFixture(t)
	advance(time.Date(2026, 5, 12, 14, 30, 0, 0, time.UTC))
	events := []ingest.Event{
		mkEvent(t, "2026-05-12T14:30:00Z", "api", "info", "a", map[string]any{"user_id": "42"}),
		mkEvent(t, "2026-05-12T14:30:01Z", "api", "info", "b", map[string]any{"user_id": "42"}),
		mkEvent(t, "2026-05-12T14:30:02Z", "api", "info", "c", map[string]any{"user_id": "99"}),
	}
	if err := mgr.Flush(ctx, events); err != nil {
		t.Fatalf("flush: %v", err)
	}
	resp, err := exec.Run(ctx, query.Request{
		From:         time.Date(2026, 5, 12, 14, 0, 0, 0, time.UTC),
		To:           time.Date(2026, 5, 12, 15, 0, 0, 0, time.UTC),
		MaxTimeRange: 24 * time.Hour,
		Facets:       []query.FacetSpec{{Name: "user_id", Field: "attributes.user_id", CardinalityCap: 25}},
	})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	got := facetMap(resp.Facets["user_id"].Values)
	want := map[string]int64{"42": 2, "99": 1}
	if !mapsEqual(got, want) {
		t.Errorf("user_id facet = %v, want %v", got, want)
	}
}

func TestExecutor_Count_ExactSmallDataset(t *testing.T) {
	ctx := context.Background()
	exec, mgr, _, advance := newFixture(t)
	advance(time.Date(2026, 5, 12, 14, 30, 0, 0, time.UTC))
	events := []ingest.Event{
		mkEvent(t, "2026-05-12T14:30:00Z", "api", "info", "a", nil),
		mkEvent(t, "2026-05-12T14:30:01Z", "api", "error", "b", nil),
		mkEvent(t, "2026-05-12T14:30:02Z", "worker", "info", "c", nil),
	}
	if err := mgr.Flush(ctx, events); err != nil {
		t.Fatalf("flush: %v", err)
	}
	count, exact, _, _, err := exec.Count(ctx, query.Request{
		From:         time.Date(2026, 5, 12, 14, 0, 0, 0, time.UTC),
		To:           time.Date(2026, 5, 12, 15, 0, 0, 0, time.UTC),
		Filter:       query.Leaf{Field: "service", Op: query.OpEq, Value: "api", CaseSensitive: true},
		MaxTimeRange: 24 * time.Hour,
	}, 10001)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if !exact || count != 2 {
		t.Errorf("count=%d exact=%v, want count=2 exact=true", count, exact)
	}
}

func TestExecutor_Count_LowerBoundOnLargeDataset(t *testing.T) {
	ctx := context.Background()
	exec, mgr, _, advance := newFixture(t)
	advance(time.Date(2026, 5, 12, 14, 30, 0, 0, time.UTC))
	const total = 50
	events := make([]ingest.Event, 0, total)
	base := time.Date(2026, 5, 12, 14, 30, 0, 0, time.UTC)
	for i := 0; i < total; i++ {
		events = append(events, mkEvent(t,
			base.Add(time.Duration(i)*time.Millisecond).Format(time.RFC3339Nano),
			"api", "info", fmt.Sprintf("m-%d", i), nil))
	}
	if err := mgr.Flush(ctx, events); err != nil {
		t.Fatalf("flush: %v", err)
	}
	// Lower the guard so we can validate the short-circuit behaviour
	// without ingesting 10k rows.
	count, exact, _, _, err := exec.Count(ctx, query.Request{
		From:         time.Date(2026, 5, 12, 14, 0, 0, 0, time.UTC),
		To:           time.Date(2026, 5, 12, 15, 0, 0, 0, time.UTC),
		MaxTimeRange: 24 * time.Hour,
	}, 10)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if exact {
		t.Errorf("exact = true, want false (hit short-circuit)")
	}
	if count != 9 { // limitGuard - 1 reported as the lower bound
		t.Errorf("count = %d, want 9 (limitGuard-1)", count)
	}
}

func facetMap(values []query.FacetValue) map[string]int64 {
	m := make(map[string]int64, len(values))
	for _, v := range values {
		m[v.Value] = v.Count
	}
	return m
}

func mapsEqual(a, b map[string]int64) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}
