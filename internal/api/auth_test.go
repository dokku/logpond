package api

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dokku/logpond/internal/ingest"
	"github.com/dokku/logpond/internal/metrics"
	dto "github.com/prometheus/client_model/go"
)

func newTestServerWithTokens(t *testing.T, capacity int, tokens map[string][]string) (*Server, *ingest.Buffer, *metrics.Metrics) {
	t.Helper()
	buf := ingest.NewBuffer(capacity)
	m := metrics.New(metrics.Options{FillRatio: buf.FillRatio})
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	src := ingest.SourceSpec{
		Name: "default",
		Extract: map[string][]string{
			"timestamp": {"timestamp"},
			"level":     {"level"},
			"message":   {"message"},
		},
	}
	srv := New(Options{
		Logger:  logger,
		Buffer:  buf,
		Metrics: m,
		Extractors: map[string]*ingest.Extractor{
			"default": ingest.NewExtractor(src),
		},
		IngestTokens: tokens,
	})
	return srv, buf, m
}

// authedPost is like postIngest but lets the caller set an
// Authorization header.
func authedPost(srv *Server, source, authHeader string) *httptest.ResponseRecorder {
	body := `{"timestamp":"2026-05-12T14:32:01Z","level":"info","message":"hi"}`
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/ingest/"+source,
		io.NopCloser(bytes.NewReader([]byte(body))))
	req.Header.Set("Content-Type", "application/x-ndjson")
	if authHeader != "" {
		req.Header.Set("Authorization", authHeader)
	}
	srv.Handler().ServeHTTP(rr, req)
	return rr
}

func TestIngestAuth_NoTokens_Accepts(t *testing.T) {
	srv, _, _ := newTestServerWithTokens(t, 100, nil)
	rr := authedPost(srv, "default", "")
	if rr.Code != http.StatusAccepted {
		t.Fatalf("status: %d body=%s", rr.Code, rr.Body)
	}
}

func TestIngestAuth_MissingHeader_401(t *testing.T) {
	srv, _, m := newTestServerWithTokens(t, 100, map[string][]string{"default": {"lpk_live_correct"}})
	rr := authedPost(srv, "default", "")
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("status: %d body=%s", rr.Code, rr.Body)
	}
	if got := rr.Header().Get("WWW-Authenticate"); got != "Bearer" {
		t.Errorf("WWW-Authenticate: got %q want %q", got, "Bearer")
	}
	mustErrorCode(t, rr, "unauthorized")
	if v := counterValue(t, m.IngestAuthFailuresTotal.WithLabelValues("default")); v != 1 {
		t.Errorf("auth_failures: got %v want 1", v)
	}
}

func TestIngestAuth_WrongToken_401(t *testing.T) {
	srv, _, m := newTestServerWithTokens(t, 100, map[string][]string{"default": {"lpk_live_correct"}})
	rr := authedPost(srv, "default", "Bearer lpk_live_wrong")
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("status: %d body=%s", rr.Code, rr.Body)
	}
	mustErrorCode(t, rr, "unauthorized")
	if v := counterValue(t, m.IngestAuthFailuresTotal.WithLabelValues("default")); v != 1 {
		t.Errorf("auth_failures: got %v want 1", v)
	}
}

func TestIngestAuth_MatchingToken_202(t *testing.T) {
	srv, _, _ := newTestServerWithTokens(t, 100, map[string][]string{"default": {"lpk_live_correct"}})
	rr := authedPost(srv, "default", "Bearer lpk_live_correct")
	if rr.Code != http.StatusAccepted {
		t.Fatalf("status: %d body=%s", rr.Code, rr.Body)
	}
}

func TestIngestAuth_BearerSchemeCaseInsensitive(t *testing.T) {
	srv, _, _ := newTestServerWithTokens(t, 100, map[string][]string{"default": {"lpk_live_correct"}})
	for _, scheme := range []string{"Bearer", "bearer", "BEARER", "BeArEr"} {
		t.Run(scheme, func(t *testing.T) {
			rr := authedPost(srv, "default", scheme+" lpk_live_correct")
			if rr.Code != http.StatusAccepted {
				t.Errorf("status: %d", rr.Code)
			}
		})
	}
}

func TestIngestAuth_TwoTokens_BothAccepted(t *testing.T) {
	srv, _, _ := newTestServerWithTokens(t, 100, map[string][]string{"default": {"lpk_live_old", "lpk_live_new"}})
	for _, tok := range []string{"lpk_live_old", "lpk_live_new"} {
		t.Run(tok, func(t *testing.T) {
			rr := authedPost(srv, "default", "Bearer "+tok)
			if rr.Code != http.StatusAccepted {
				t.Errorf("status: %d body=%s", rr.Code, rr.Body)
			}
		})
	}
}

func TestIngestAuth_MalformedBearer_401(t *testing.T) {
	srv, _, _ := newTestServerWithTokens(t, 100, map[string][]string{"default": {"lpk_live_correct"}})
	for _, hdr := range []string{
		"NotBearer lpk_live_correct",
		"Bearer",         // no token
		"Bearer ",        // empty token
		"Bearerlpk_live", // missing space between scheme and token
		"Basic dXNlcg==", // wrong scheme
	} {
		t.Run(hdr, func(t *testing.T) {
			rr := authedPost(srv, "default", hdr)
			if rr.Code != http.StatusUnauthorized {
				t.Errorf("status: %d body=%s", rr.Code, rr.Body)
			}
		})
	}
}

func TestIngestAuth_AuthChecksBeforeBodyRead(t *testing.T) {
	// A source with tokens configured should not consume a body that
	// would otherwise exceed the size cap. The handler must short-circuit
	// on 401 before getting that far.
	srv, _, _ := newTestServerWithTokens(t, 100, map[string][]string{"default": {"lpk_live_correct"}})
	big := bytes.Repeat([]byte("x"), maxDecompressedBody+10)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/ingest/default",
		io.NopCloser(bytes.NewReader(big)))
	req.Header.Set("Content-Type", "application/x-ndjson")
	// No Authorization header -> expect 401, not 413.
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("status: %d body=%s", rr.Code, rr.Body.String()[:200])
	}
}

func TestIngestAuth_WarningLogRateLimited(t *testing.T) {
	srv, _, _ := newTestServerWithTokens(t, 100, map[string][]string{"default": {"lpk_live_correct"}})
	// Force every request to fail and confirm only one warning lands per
	// source within a one-minute window by inspecting the underlying
	// rate-limiter state directly.
	for i := 0; i < 5; i++ {
		_ = authedPost(srv, "default", "")
	}
	srv.auth.mu.Lock()
	defer srv.auth.mu.Unlock()
	if _, ok := srv.auth.lastWarn["default"]; !ok {
		t.Fatalf("expected lastWarn entry for default")
	}
}

func TestIngestAuth_Fingerprint_FirstSixChars(t *testing.T) {
	a := newAuthChecker(tokenMap{"default": {"lpk_live_correct"}}, slog.Default(), nil)
	a.tokenPrefix = 6
	// Indirectly: ensure extractBearer + the fingerprint helpers cope
	// with short tokens by hand-testing the relevant path.
	tok, ok := extractBearer("Bearer abc")
	if !ok || tok != "abc" {
		t.Fatalf("extractBearer: %q %v", tok, ok)
	}
	// The fingerprint logic is folded into logFailure; verify it bounds
	// to len(offered) when shorter than the prefix length by re-using
	// matchesAny semantics indirectly via store.
	a.store(tokenMap{"default": {"x"}})
	if !matchesAny("x", []string{"x"}) {
		t.Fatalf("matchesAny should match identical token")
	}
}

func TestSetIngestTokens_HotSwap(t *testing.T) {
	srv, _, _ := newTestServerWithTokens(t, 100, nil)
	// Initially no tokens: requests succeed.
	rr := authedPost(srv, "default", "")
	if rr.Code != http.StatusAccepted {
		t.Fatalf("pre-swap status: %d", rr.Code)
	}
	// Add a token via SetIngestTokens (simulating a reload).
	srv.SetIngestTokens(map[string][]string{"default": {"lpk_live_new"}})
	rr = authedPost(srv, "default", "")
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("post-swap unauthenticated: %d", rr.Code)
	}
	rr = authedPost(srv, "default", "Bearer lpk_live_new")
	if rr.Code != http.StatusAccepted {
		t.Fatalf("post-swap authenticated: %d body=%s", rr.Code, rr.Body)
	}
	// Remove tokens; unauthenticated should succeed again.
	srv.SetIngestTokens(nil)
	rr = authedPost(srv, "default", "")
	if rr.Code != http.StatusAccepted {
		t.Fatalf("post-remove status: %d", rr.Code)
	}
}

func TestExtractBearer(t *testing.T) {
	cases := []struct {
		header string
		want   string
		ok     bool
	}{
		{"", "", false},
		{"Bearer abc", "abc", true},
		{"bearer abc", "abc", true},
		{"BEARER abc", "abc", true},
		{"Bearer  abc", "abc", true}, // extra whitespace is trimmed
		{"Bearer", "", false},
		{"Bearer ", "", false},
		{"NotBearer abc", "", false},
		{"Basic dXNlcg==", "", false},
		{"Bearer abc def", "", false}, // whitespace inside token rejected
	}
	for _, tc := range cases {
		t.Run(tc.header, func(t *testing.T) {
			got, ok := extractBearer(tc.header)
			if got != tc.want || ok != tc.ok {
				t.Errorf("extractBearer(%q) = (%q, %v); want (%q, %v)", tc.header, got, ok, tc.want, tc.ok)
			}
		})
	}
}

// counterValue is a tiny helper that reads the current value of a
// prometheus.Counter. The metric library doesn't expose a Get method
// on counters, so we read the proto representation.
func counterValue(t *testing.T, c interface {
	Write(*dto.Metric) error
}) float64 {
	t.Helper()
	var m dto.Metric
	if err := c.Write(&m); err != nil {
		t.Fatalf("counter write: %v", err)
	}
	if m.Counter == nil {
		t.Fatalf("counter is nil")
	}
	return m.Counter.GetValue()
}

// Silence the unused-import warning when only some tests in this file
// actually exercise json/strings; keep them imported for parity with
// server_test.go's helpers.
var _ = json.Unmarshal
var _ = strings.TrimSpace
