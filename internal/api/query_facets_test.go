package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/dokku/logpond/internal/catalog"
	"github.com/dokku/logpond/internal/config"
	"github.com/dokku/logpond/internal/facets"
	"github.com/dokku/logpond/internal/ingest"
	"github.com/dokku/logpond/internal/metrics"
	"github.com/dokku/logpond/internal/query"
	"github.com/dokku/logpond/internal/segments"
)

func newQueryServerWithData(t *testing.T) *Server {
	t.Helper()
	dir := t.TempDir()
	ctx := context.Background()
	cat, err := catalog.Open(ctx, filepath.Join(dir, "catalog.db"), nil)
	if err != nil {
		t.Fatalf("open catalog: %v", err)
	}
	t.Cleanup(func() { _ = cat.Close() })

	now := time.Date(2026, 5, 12, 14, 30, 0, 0, time.UTC)
	mgr, err := segments.New(segments.Options{
		DataDir:     dir,
		Window:      time.Hour,
		SealGrace:   time.Second,
		MemoryLimit: "128MB",
		Catalog:     cat,
		Now:         func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Close() })

	events := []ingest.Event{
		mkAPIEvent(t, "2026-05-12T14:30:00Z", "api", "info", "a"),
		mkAPIEvent(t, "2026-05-12T14:30:01Z", "api", "error", "b"),
		mkAPIEvent(t, "2026-05-12T14:30:02Z", "worker", "info", "c"),
	}
	if err := mgr.Flush(ctx, events); err != nil {
		t.Fatalf("flush: %v", err)
	}

	exec := query.NewExecutor(cat, mgr, nil)
	reg := facets.New(facets.Options{Catalog: cat})
	if err := reg.Load(ctx, config.Defaults()); err != nil {
		t.Fatalf("load registry: %v", err)
	}
	buf := ingest.NewBuffer(10)
	m := metrics.New(metrics.Options{FillRatio: buf.FillRatio})
	return New(Options{
		Buffer:       buf,
		Metrics:      m,
		Extractors:   map[string]*ingest.Extractor{},
		Executor:     exec,
		Facets:       reg,
		MaxTimeRange: 24 * time.Hour,
	})
}

func mkAPIEvent(t *testing.T, ts, service, level, message string) ingest.Event {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339Nano, ts)
	if err != nil {
		t.Fatalf("parse ts %q: %v", ts, err)
	}
	return ingest.Event{
		Timestamp:  parsed.UTC(),
		Service:    service,
		Level:      level,
		Message:    message,
		Source:     "default",
		Attributes: map[string]any{},
		Raw:        `{"service":"` + service + `"}`,
	}
}

func TestQuery_ReturnsFacetsForBuiltins(t *testing.T) {
	srv := newQueryServerWithData(t)
	body := bytes.NewBufferString(`{"time_range":{"from":"2026-05-12T14:00:00Z","to":"2026-05-12T15:00:00Z"}}`)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/query", body)
	req.Header.Set("Content-Type", "application/json")
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status %d body=%s", rr.Code, rr.Body)
	}
	var resp queryResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Events) != 3 {
		t.Errorf("events = %d, want 3", len(resp.Events))
	}
	svc, ok := resp.Facets["service"]
	if !ok {
		t.Fatal("service facet missing")
	}
	got := map[string]int64{}
	for _, v := range svc.Values {
		got[v.Value] = v.Count
	}
	want := map[string]int64{"api": 2, "worker": 1}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("service[%q] = %d, want %d", k, got[k], v)
		}
	}
}

func TestQueryCount_ExactSmallResult(t *testing.T) {
	srv := newQueryServerWithData(t)
	body := bytes.NewBufferString(`{"time_range":{"from":"2026-05-12T14:00:00Z","to":"2026-05-12T15:00:00Z"},"filter":{"field":"service","op":"eq","value":"api"}}`)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/query/count", body)
	req.Header.Set("Content-Type", "application/json")
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status %d body=%s", rr.Code, rr.Body)
	}
	var resp countResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !resp.Exact || resp.Count != 2 {
		t.Errorf("count=%d exact=%v, want 2/true", resp.Count, resp.Exact)
	}
}

func TestSearchSuggest_ValueContext_ReturnsSuggestions(t *testing.T) {
	srv := newQueryServerWithData(t)
	body := bytes.NewBufferString(`{"q":"service:","cursor_pos":8,"time_range":{"from":"2026-05-12T14:00:00Z","to":"2026-05-12T15:00:00Z"}}`)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/search-suggest", body)
	req.Header.Set("Content-Type", "application/json")
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status %d body=%s", rr.Code, rr.Body)
	}
	var resp suggestResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Context != "value" || resp.Field != "service" {
		t.Errorf("context/field = %s/%s, want value/service", resp.Context, resp.Field)
	}
	if len(resp.Suggestions) == 0 {
		t.Error("expected at least one value suggestion")
	}
}

func TestSearchSuggest_FieldContext(t *testing.T) {
	srv := newQueryServerWithData(t)
	body := bytes.NewBufferString(`{"q":"se","cursor_pos":2}`)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/search-suggest", body)
	req.Header.Set("Content-Type", "application/json")
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status %d body=%s", rr.Code, rr.Body)
	}
	var resp suggestResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Context != "field" {
		t.Errorf("context = %s, want field", resp.Context)
	}
	found := false
	for _, s := range resp.Suggestions {
		if s.Text == "service" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("service not in suggestions: %+v", resp.Suggestions)
	}
}

func TestSearchSuggest_CombinatorContext(t *testing.T) {
	srv := newQueryServerWithData(t)
	body := bytes.NewBufferString(`{"q":"service:api ","cursor_pos":12}`)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/search-suggest", body)
	req.Header.Set("Content-Type", "application/json")
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status %d body=%s", rr.Code, rr.Body)
	}
	var resp suggestResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Context != "combinator" {
		t.Errorf("context = %s, want combinator", resp.Context)
	}
}
