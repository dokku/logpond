package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/dokku/logpond/internal/facets"
	"github.com/go-chi/chi/v5"
)

// facetDef is the wire shape used by the §13.16-§13.19 CRUD endpoints.
type facetDef struct {
	Name           string  `json:"name"`
	Field          string  `json:"field"`
	DisplayLabel   string  `json:"display_label"`
	CardinalityCap int     `json:"cardinality_cap"`
	ValueType      string  `json:"value_type"`
	Kind           string  `json:"kind"`
	Source         *string `json:"source"`
	CreatedAt      string  `json:"created_at,omitempty"`
	UpdatedAt      string  `json:"updated_at,omitempty"`
}

func definitionToDTO(d facets.Definition) facetDef {
	out := facetDef{
		Name:           d.Name,
		Field:          d.Field,
		DisplayLabel:   d.DisplayLabel,
		CardinalityCap: d.CardinalityCap,
		ValueType:      d.ValueType,
		Kind:           string(d.Kind),
	}
	if d.Source != "" {
		s := string(d.Source)
		out.Source = &s
	}
	if !d.CreatedAt.IsZero() {
		out.CreatedAt = d.CreatedAt.UTC().Format(time.RFC3339)
	}
	if !d.UpdatedAt.IsZero() {
		out.UpdatedAt = d.UpdatedAt.UTC().Format(time.RFC3339)
	}
	return out
}

func (s *Server) handleListFacets(w http.ResponseWriter, r *http.Request) {
	if s.facets == nil {
		writeError(w, http.StatusServiceUnavailable, "internal_error", "facets registry not configured", nil)
		return
	}
	defs := s.facets.List()
	out := struct {
		Facets []facetDef `json:"facets"`
	}{Facets: make([]facetDef, 0, len(defs))}
	for _, d := range defs {
		out.Facets = append(out.Facets, definitionToDTO(d))
	}
	writeJSON(w, http.StatusOK, out)
}

type createFacetRequest struct {
	Name           string `json:"name"`
	Field          string `json:"field"`
	DisplayLabel   string `json:"display_label"`
	CardinalityCap int    `json:"cardinality_cap"`
	ValueType      string `json:"value_type"`
}

func (s *Server) handleCreateFacet(w http.ResponseWriter, r *http.Request) {
	if s.facets == nil {
		writeError(w, http.StatusServiceUnavailable, "internal_error", "facets registry not configured", nil)
		return
	}
	defer r.Body.Close()
	var req createFacetRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", "invalid JSON: "+err.Error(), nil)
		return
	}
	def, err := s.facets.CreateUI(r.Context(), facets.CreateInput{
		Name:           req.Name,
		Field:          req.Field,
		DisplayLabel:   req.DisplayLabel,
		CardinalityCap: req.CardinalityCap,
		ValueType:      req.ValueType,
	})
	if err != nil {
		s.writeFacetError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, definitionToDTO(def))
}

type patchFacetRequest struct {
	DisplayLabel   *string `json:"display_label"`
	CardinalityCap *int    `json:"cardinality_cap"`
}

func (s *Server) handlePatchFacet(w http.ResponseWriter, r *http.Request) {
	if s.facets == nil {
		writeError(w, http.StatusServiceUnavailable, "internal_error", "facets registry not configured", nil)
		return
	}
	name := chi.URLParam(r, "name")
	defer r.Body.Close()
	var req patchFacetRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", "invalid JSON: "+err.Error(), nil)
		return
	}
	def, err := s.facets.UpdateUI(r.Context(), name, facets.PatchInput{
		DisplayLabel:   req.DisplayLabel,
		CardinalityCap: req.CardinalityCap,
	})
	if err != nil {
		s.writeFacetError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, definitionToDTO(def))
}

func (s *Server) handleDeleteFacet(w http.ResponseWriter, r *http.Request) {
	if s.facets == nil {
		writeError(w, http.StatusServiceUnavailable, "internal_error", "facets registry not configured", nil)
		return
	}
	name := chi.URLParam(r, "name")
	if err := s.facets.DeleteUI(r.Context(), name); err != nil {
		s.writeFacetError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) writeFacetError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, facets.ErrConflict):
		writeError(w, http.StatusConflict, "facet_conflict", err.Error(), nil)
	case errors.Is(err, facets.ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", err.Error(), nil)
	case errors.Is(err, facets.ErrInvalid):
		writeError(w, http.StatusBadRequest, "bad_request", err.Error(), nil)
	default:
		s.logger.Error("facet operation failed", "err", err)
		writeError(w, http.StatusInternalServerError, "internal_error", err.Error(), nil)
	}
}
