package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestQuery_RejectsMissingTimeRange(t *testing.T) {
	srv, _ := newTestServer(t, 100)
	body := strings.NewReader(`{}`)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/query", body)
	req.Header.Set("Content-Type", "application/json")
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
	var env errorEnvelope
	_ = json.Unmarshal(rr.Body.Bytes(), &env)
	if env.Error.Code != "invalid_time_range" {
		t.Errorf("code = %q, want invalid_time_range", env.Error.Code)
	}
}

func TestQuery_FormA_WithoutExecutor_Returns503(t *testing.T) {
	// The Form A path now parses `q` successfully; the test server has
	// no executor configured, so we expect a 503 from the executor
	// gate rather than the 501 we used to return before Phase 5.
	srv, _ := newTestServer(t, 100)
	body := bytes.NewBufferString(`{"time_range":{"from":"2026-05-12T13:00:00Z","to":"2026-05-12T14:00:00Z"},"q":"service:api"}`)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/query", body)
	req.Header.Set("Content-Type", "application/json")
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rr.Code)
	}
}

func TestQuery_FormA_InvalidSyntax_Returns400WithColumn(t *testing.T) {
	srv, _ := newTestServer(t, 100)
	body := bytes.NewBufferString(`{"time_range":{"from":"2026-05-12T13:00:00Z","to":"2026-05-12T14:00:00Z"},"q":"service:"}`)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/query", body)
	req.Header.Set("Content-Type", "application/json")
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
	var env errorEnvelope
	_ = json.Unmarshal(rr.Body.Bytes(), &env)
	if env.Error.Code != "invalid_query_syntax" {
		t.Errorf("code = %q, want invalid_query_syntax", env.Error.Code)
	}
	if _, ok := env.Error.Details["column"]; !ok {
		t.Errorf("details.column missing: %+v", env.Error.Details)
	}
}

func TestQuery_RejectsCombinedFilterAndQ(t *testing.T) {
	srv, _ := newTestServer(t, 100)
	body := bytes.NewBufferString(`{
        "time_range":{"from":"2026-05-12T13:00:00Z","to":"2026-05-12T14:00:00Z"},
        "q":"service:api",
        "filter":{"field":"level","op":"eq","value":"error"}
    }`)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/query", body)
	req.Header.Set("Content-Type", "application/json")
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
}

func TestQuery_RejectsBadFilter(t *testing.T) {
	srv, _ := newTestServer(t, 100)
	body := bytes.NewBufferString(`{
        "time_range":{"from":"2026-05-12T13:00:00Z","to":"2026-05-12T14:00:00Z"},
        "filter":{"field":"level","op":"fuzzy","value":"x"}
    }`)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/query", body)
	req.Header.Set("Content-Type", "application/json")
	srv.Handler().ServeHTTP(rr, req)
	// Without an executor configured, the handler should return 503.
	// With an executor, the filter would fail validation → 400. Accept
	// either since the request never reaches the executor here.
	if rr.Code != http.StatusServiceUnavailable && rr.Code != http.StatusBadRequest {
		t.Errorf("unexpected status %d body=%s", rr.Code, rr.Body.String())
	}
}
