package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHealthz_HealthCheckFailure_Returns503(t *testing.T) {
	srv, _ := newTestServer(t, 100)
	srv.healthCheck = func(ctx context.Context) error {
		return errors.New("ring buffer stuck")
	}
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status: %d", rr.Code)
	}
	var body struct {
		Status string `json:"status"`
		Reason string `json:"reason"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Status != "unhealthy" {
		t.Errorf("status: %s", body.Status)
	}
	if !strings.Contains(body.Reason, "stuck") {
		t.Errorf("reason: %s", body.Reason)
	}
}

func TestMetrics_ServesPrometheusFormat(t *testing.T) {
	srv, _ := newTestServer(t, 100)
	// Seed every vector with at least one labeled observation so the
	// Prometheus text exposition format emits the metric family.
	srv.metrics.IngestEventsTotal.WithLabelValues("default").Add(0)
	srv.metrics.IngestSkippedLinesTotal.WithLabelValues("default", "parse_error").Add(0)
	srv.metrics.ArchiveScriptInvocationsTotal.WithLabelValues("archive", "0").Add(0)
	srv.metrics.Segments.WithLabelValues("sealed").Set(0)
	srv.metrics.DiskBytes.WithLabelValues("sealed").Set(0)
	srv.metrics.Facets.WithLabelValues("builtin", "").Set(0)
	srv.metrics.ArchiveScriptDuration.WithLabelValues("archive").Observe(0)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status: %d body=%s", rr.Code, rr.Body)
	}
	body := rr.Body.String()
	want := []string{
		"logpond_ingest_events_total",
		"logpond_ingest_skipped_lines_total",
		"logpond_ring_buffer_fill_ratio",
		"logpond_archive_script_invocations_total",
		"logpond_segments",
		"logpond_disk_bytes",
		"logpond_facets",
		"logpond_live_tail_clients",
		"logpond_query_duration_seconds",
		"logpond_facet_compute_duration_seconds",
		"logpond_search_suggest_duration_seconds",
		"logpond_query_count_duration_seconds",
		"logpond_search_bar_parse_duration_seconds",
		"logpond_archive_script_duration_seconds",
	}
	for _, w := range want {
		if !strings.Contains(body, w) {
			t.Errorf("metrics output missing %q", w)
		}
	}
}

func TestReload_NotConfigured_Returns503(t *testing.T) {
	srv, _ := newTestServer(t, 100)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/admin/reload", strings.NewReader("{}"))
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status: %d", rr.Code)
	}
}

func TestReload_DelegatesToHandler(t *testing.T) {
	srv, _ := newTestServer(t, 100)
	called := false
	srv.reloadHandler = func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"reloaded":true}`))
	}
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/admin/reload", strings.NewReader("{}"))
	srv.Handler().ServeHTTP(rr, req)
	if !called {
		t.Fatalf("reload handler not invoked")
	}
	if rr.Code != http.StatusOK {
		t.Fatalf("status: %d", rr.Code)
	}
}
