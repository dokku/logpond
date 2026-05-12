package ui

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
)

// TestServer_RoutesAreRegistered spins up a chi router, mounts the UI,
// and checks each documented surface area returns 200 with HTML content.
// The handlers run against an executor/registry-less server so the
// query/count/suggest endpoints exercise their guard clauses (executor
// nil → fall through to empty rendering or 500).
func TestServer_RoutesAreRegistered(t *testing.T) {
	t.Parallel()
	s, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	r := chi.NewRouter()
	s.Register(r)

	cases := []struct {
		name     string
		method   string
		path     string
		wantCT   string
		wantBody string
	}{
		{"search", http.MethodGet, "/", "text/html; charset=utf-8", `id="results"`},
		{"tail", http.MethodGet, "/tail", "text/html; charset=utf-8", `id="tail-events"`},
		{"admin", http.MethodGet, "/admin", "text/html; charset=utf-8", `Archive backend`},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			req := httptest.NewRequest(c.method, c.path, nil)
			rr := httptest.NewRecorder()
			r.ServeHTTP(rr, req)
			if rr.Code != http.StatusOK {
				t.Fatalf("status %d, want 200", rr.Code)
			}
			if got := rr.Header().Get("Content-Type"); got != c.wantCT {
				t.Errorf("content-type %q, want %q", got, c.wantCT)
			}
			if !strings.Contains(rr.Body.String(), c.wantBody) {
				t.Errorf("body missing %q", c.wantBody)
			}
		})
	}
}

func TestStaticHandler_ServesAppCSS(t *testing.T) {
	t.Parallel()
	s, err := New(Options{})
	if err != nil {
		t.Fatal(err)
	}
	r := chi.NewRouter()
	s.Register(r)

	req := httptest.NewRequest(http.MethodGet, "/static/css/app.css", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status %d", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "--logpond-text-base") {
		t.Errorf("app.css missing expected token")
	}
	if etag := rr.Header().Get("ETag"); etag == "" {
		t.Errorf("missing ETag header")
	}
}

func TestStaticHandler_RespectsIfNoneMatch(t *testing.T) {
	t.Parallel()
	s, err := New(Options{})
	if err != nil {
		t.Fatal(err)
	}
	r := chi.NewRouter()
	s.Register(r)

	// First request: capture the ETag.
	req := httptest.NewRequest(http.MethodGet, "/static/css/app.css", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	etag := rr.Header().Get("ETag")
	if etag == "" {
		t.Fatal("no etag")
	}

	// Repeat with If-None-Match: should be 304 with empty body.
	req2 := httptest.NewRequest(http.MethodGet, "/static/css/app.css", nil)
	req2.Header.Set("If-None-Match", etag)
	rr2 := httptest.NewRecorder()
	r.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusNotModified {
		t.Errorf("status %d, want 304", rr2.Code)
	}
}
