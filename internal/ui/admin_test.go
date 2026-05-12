package ui

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dokku/logpond/internal/catalog"
	"github.com/dokku/logpond/internal/config"
	"github.com/dokku/logpond/internal/facets"
	"github.com/go-chi/chi/v5"
)

// newAdminServer builds a Server backed by an in-temp-dir catalog and a
// freshly-loaded facets registry. No archive backend is wired so the
// archive HTMX endpoints return their guard-path responses.
func newAdminServer(t *testing.T) (*Server, *catalog.Catalog) {
	t.Helper()
	cat, err := catalog.Open(context.Background(), filepath.Join(t.TempDir(), "catalog.db"), nil)
	if err != nil {
		t.Fatalf("open catalog: %v", err)
	}
	t.Cleanup(func() { _ = cat.Close() })
	reg := facets.New(facets.Options{Catalog: cat})
	if err := reg.Load(context.Background(), config.Defaults()); err != nil {
		t.Fatalf("load facets: %v", err)
	}
	s, err := New(Options{
		Catalog: cat,
		Facets:  reg,
		Archive: ArchiveInfo{Kind: "s3", Detail: "s3://logs", Bucket: "logs"},
		RetentionInfo: RetentionInfo{
			MaxAge: "30d", MaxSize: "20GB", ArchiveBeforeDelete: true,
		},
		FacetSampleSize: 24,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s, cat
}

// mountAdmin builds a chi router with the server attached and returns
// it. Lets tests fire requests against /admin and /ui/admin/* with
// real routing semantics.
func mountAdmin(t *testing.T, s *Server) http.Handler {
	t.Helper()
	r := chi.NewRouter()
	s.Register(r)
	return r
}

func TestAdminPage_RendersAllSections(t *testing.T) {
	t.Parallel()
	s, _ := newAdminServer(t)
	r := mountAdmin(t, s)

	req := httptest.NewRequest(http.MethodGet, "/admin", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status %d body=%s", rr.Code, rr.Body.String())
	}
	html := rr.Body.String()
	for _, want := range []string{
		"Retention policy",
		`hx-post="/ui/admin/retention/run?dry_run=true"`,
		`hx-post="/ui/admin/retention/run?dry_run=false"`,
		"Archive backend",
		`hx-post="/ui/admin/archive/verify"`,
		"+ Add facet",
		"Storage",
		`hx-get="/ui/admin/storage"`,
		"Segments",
		`hx-get="/ui/admin/segments"`,
		"+ Import segment",
		"adminView()",
	} {
		if !strings.Contains(html, want) {
			t.Errorf("admin page missing %q", want)
		}
	}
	// S3 variant: no script-only Test invocation button.
	if strings.Contains(html, "Test invocation") {
		t.Errorf("S3 variant should not show Test invocation button")
	}
}

func TestAdminPage_ScriptBackendVariant(t *testing.T) {
	t.Parallel()
	cat, err := catalog.Open(context.Background(), filepath.Join(t.TempDir(), "catalog.db"), nil)
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	t.Cleanup(func() { _ = cat.Close() })
	reg := facets.New(facets.Options{Catalog: cat})
	_ = reg.Load(context.Background(), config.Defaults())
	s, err := New(Options{
		Catalog: cat,
		Facets:  reg,
		Archive: ArchiveInfo{Kind: "script", Detail: "/etc/logpond/archive.sh", Path: "/etc/logpond/archive.sh", Timeout: "10m"},
		RetentionInfo: RetentionInfo{MaxAge: "30d"},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	r := mountAdmin(t, s)

	req := httptest.NewRequest(http.MethodGet, "/admin", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status %d", rr.Code)
	}
	html := rr.Body.String()
	for _, want := range []string{
		"/etc/logpond/archive.sh",
		`hx-post="/ui/admin/archive/test"`,
		"Test invocation",
		"Capabilities",
	} {
		if !strings.Contains(html, want) {
			t.Errorf("script variant missing %q", want)
		}
	}
}

func TestAdminFacets_AddDeleteFlow(t *testing.T) {
	t.Parallel()
	s, _ := newAdminServer(t)
	r := mountAdmin(t, s)

	// Add a new ui-source facet via the form endpoint.
	form := url.Values{}
	form.Set("name", "user_id")
	form.Set("field", "attributes.user.id")
	form.Set("display_label", "User ID")
	form.Set("cardinality_cap", "25")
	form.Set("value_type", "string")
	req := httptest.NewRequest(http.MethodPost, "/ui/admin/facets", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("create status %d body=%s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "user_id") {
		t.Errorf("create response missing new facet; got %s", rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), `hx-delete="/ui/admin/facets/user_id"`) {
		t.Errorf("response missing delete affordance")
	}

	// Delete the same facet — should return the table without it.
	req = httptest.NewRequest(http.MethodDelete, "/ui/admin/facets/user_id", nil)
	rr = httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("delete status %d body=%s", rr.Code, rr.Body.String())
	}
	if strings.Contains(rr.Body.String(), "user_id") {
		t.Errorf("delete response still mentions user_id")
	}
}

func TestAdminFacets_DeleteBuiltinReturnsConflict(t *testing.T) {
	t.Parallel()
	s, _ := newAdminServer(t)
	r := mountAdmin(t, s)
	req := httptest.NewRequest(http.MethodDelete, "/ui/admin/facets/service", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	if rr.Code != http.StatusConflict {
		t.Errorf("delete builtin status %d, want 409", rr.Code)
	}
}

func TestAdminStorage_FragmentRenders(t *testing.T) {
	t.Parallel()
	s, _ := newAdminServer(t)
	r := mountAdmin(t, s)
	req := httptest.NewRequest(http.MethodGet, "/ui/admin/storage", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status %d", rr.Code)
	}
	for _, want := range []string{"Total local", "Sealed (local)", "Archived (off-host)"} {
		if !strings.Contains(rr.Body.String(), want) {
			t.Errorf("storage fragment missing %q", want)
		}
	}
}

func TestAdminSegments_EmptyState(t *testing.T) {
	t.Parallel()
	s, _ := newAdminServer(t)
	r := mountAdmin(t, s)
	req := httptest.NewRequest(http.MethodGet, "/ui/admin/segments", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status %d", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "No segments match") {
		t.Errorf("empty-state copy missing; got %s", rr.Body.String())
	}
}

func TestAdminArchiveVerify_RendersFragmentWithoutBackend(t *testing.T) {
	t.Parallel()
	s, _ := newAdminServer(t)
	r := mountAdmin(t, s)
	req := httptest.NewRequest(http.MethodPost, "/ui/admin/archive/verify", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status %d", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "archive backend not configured") {
		t.Errorf("expected guard message; got %s", rr.Body.String())
	}
}

func TestAdminRetentionRun_GuardWhenUnconfigured(t *testing.T) {
	t.Parallel()
	s, _ := newAdminServer(t)
	r := mountAdmin(t, s)
	req := httptest.NewRequest(http.MethodPost, "/ui/admin/retention/run?dry_run=true", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status %d", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "retention evaluator not configured") {
		t.Errorf("expected guard message; got %s", rr.Body.String())
	}
}
