package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/dokku/logpond/internal/catalog"
	"github.com/dokku/logpond/internal/config"
	"github.com/dokku/logpond/internal/facets"
	"github.com/dokku/logpond/internal/ingest"
	"github.com/dokku/logpond/internal/metrics"
)

func newFacetServer(t *testing.T) *Server {
	t.Helper()
	cat, err := catalog.Open(context.Background(), filepath.Join(t.TempDir(), "catalog.db"), nil)
	if err != nil {
		t.Fatalf("open catalog: %v", err)
	}
	t.Cleanup(func() { _ = cat.Close() })
	reg := facets.New(facets.Options{Catalog: cat})
	if err := reg.Load(context.Background(), config.Defaults()); err != nil {
		t.Fatalf("load registry: %v", err)
	}
	buf := ingest.NewBuffer(10)
	m := metrics.New(metrics.Options{FillRatio: buf.FillRatio})
	return New(Options{
		Buffer:     buf,
		Metrics:    m,
		Extractors: map[string]*ingest.Extractor{},
		Facets:     reg,
	})
}

func TestFacets_ListIncludesBuiltins(t *testing.T) {
	srv := newFacetServer(t)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/facets", nil)
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status %d body=%s", rr.Code, rr.Body)
	}
	var body struct {
		Facets []facetDef `json:"facets"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(body.Facets) != 4 {
		t.Errorf("want 4 builtin facets, got %d", len(body.Facets))
	}
	for _, f := range body.Facets {
		if f.Kind != "builtin" {
			t.Errorf("facet %s kind = %s, want builtin", f.Name, f.Kind)
		}
	}
}

func TestFacets_CreateUIFacet(t *testing.T) {
	srv := newFacetServer(t)
	body := bytes.NewBufferString(`{"name":"request_id","field":"attributes.request_id","display_label":"Request","cardinality_cap":25}`)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/facets", body)
	req.Header.Set("Content-Type", "application/json")
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusCreated {
		t.Fatalf("status %d body=%s", rr.Code, rr.Body)
	}
	var def facetDef
	if err := json.Unmarshal(rr.Body.Bytes(), &def); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if def.Kind != "custom" || def.Source == nil || *def.Source != "ui" {
		t.Errorf("kind/source = %s/%v, want custom/ui", def.Kind, def.Source)
	}
}

func TestFacets_CreateConflict_409(t *testing.T) {
	srv := newFacetServer(t)
	body := bytes.NewBufferString(`{"name":"service","field":"service"}`)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/facets", body)
	req.Header.Set("Content-Type", "application/json")
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusConflict {
		t.Fatalf("status %d body=%s", rr.Code, rr.Body)
	}
	mustErrorCode(t, rr, "facet_conflict")
}

func TestFacets_PatchUpdatesCap(t *testing.T) {
	srv := newFacetServer(t)
	createBody := bytes.NewBufferString(`{"name":"request_id","field":"attributes.request_id","cardinality_cap":10}`)
	createReq := httptest.NewRequest(http.MethodPost, "/api/facets", createBody)
	createReq.Header.Set("Content-Type", "application/json")
	srv.Handler().ServeHTTP(httptest.NewRecorder(), createReq)

	body := bytes.NewBufferString(`{"cardinality_cap":99,"display_label":"Req"}`)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPatch, "/api/facets/request_id", body)
	req.Header.Set("Content-Type", "application/json")
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status %d body=%s", rr.Code, rr.Body)
	}
	var def facetDef
	if err := json.Unmarshal(rr.Body.Bytes(), &def); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if def.CardinalityCap != 99 {
		t.Errorf("cap = %d, want 99", def.CardinalityCap)
	}
	if def.DisplayLabel != "Req" {
		t.Errorf("label = %s, want Req", def.DisplayLabel)
	}
}

func TestFacets_PatchBuiltin_409(t *testing.T) {
	srv := newFacetServer(t)
	body := bytes.NewBufferString(`{"cardinality_cap":5}`)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPatch, "/api/facets/service", body)
	req.Header.Set("Content-Type", "application/json")
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusConflict {
		t.Fatalf("status %d body=%s", rr.Code, rr.Body)
	}
}

func TestFacets_DeleteUIFacet(t *testing.T) {
	srv := newFacetServer(t)
	createBody := bytes.NewBufferString(`{"name":"request_id","field":"attributes.request_id"}`)
	createReq := httptest.NewRequest(http.MethodPost, "/api/facets", createBody)
	createReq.Header.Set("Content-Type", "application/json")
	srv.Handler().ServeHTTP(httptest.NewRecorder(), createReq)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodDelete, "/api/facets/request_id", nil)
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("status %d body=%s", rr.Code, rr.Body)
	}
	// Builtin deletes return 409.
	rr = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodDelete, "/api/facets/service", nil)
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusConflict {
		t.Fatalf("delete builtin: status %d", rr.Code)
	}
}
