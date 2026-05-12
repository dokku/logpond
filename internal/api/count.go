package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/dokku/logpond/internal/query"
	"github.com/dokku/logpond/internal/query/parser"
)

// countLimitGuard is the soft cap above which /api/query/count
// short-circuits with exact=false (PRD §13.5: counts > 10,000 return a
// lower-bound indicator).
const countLimitGuard = 10001

type countResponse struct {
	Count      int64 `json:"count"`
	Exact      bool  `json:"exact"`
	LowerBound bool  `json:"lower_bound,omitempty"`
	DurationMs int64 `json:"duration_ms"`
}

func (s *Server) handleQueryCount(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var req queryRequest
	dec := json.NewDecoder(r.Body)
	dec.UseNumber()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", "invalid JSON: "+err.Error(), nil)
		return
	}
	if req.TimeRange == nil || req.TimeRange.From.IsZero() || req.TimeRange.To.IsZero() {
		writeError(w, http.StatusBadRequest, "invalid_time_range", "time_range.from and time_range.to are required", nil)
		return
	}

	forms := 0
	if req.Q != "" {
		forms++
	}
	if len(req.Filter) > 0 {
		forms++
	}
	if len(req.Filters) > 0 {
		forms++
	}
	if forms > 1 {
		writeError(w, http.StatusBadRequest, "bad_request", "q, filter, and filters are mutually exclusive", nil)
		return
	}

	var root query.Node
	search := req.Search
	if req.Q != "" {
		pres, err := parser.Parse(req.Q)
		if err != nil {
			var pe parser.ParseError
			if errors.As(err, &pe) {
				writeError(w, http.StatusBadRequest, "invalid_query_syntax", pe.Msg, map[string]any{"column": pe.Col})
				return
			}
			writeError(w, http.StatusBadRequest, "invalid_query_syntax", err.Error(), nil)
			return
		}
		if pres.Filter != nil {
			root = pres.Filter
		}
		if pres.Search != "" {
			if search != "" {
				search = search + " " + pres.Search
			} else {
				search = pres.Search
			}
		}
	} else if len(req.Filter) > 0 {
		n, err := query.UnmarshalNode(req.Filter)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_filter", err.Error(), nil)
			return
		}
		root = n
	} else if len(req.Filters) > 0 {
		nodes := make([]query.Node, 0, len(req.Filters))
		for i, raw := range req.Filters {
			n, err := query.UnmarshalNode(raw)
			if err != nil {
				writeError(w, http.StatusBadRequest, "invalid_filter", fmt.Sprintf("filters[%d]: %s", i, err), nil)
				return
			}
			nodes = append(nodes, n)
		}
		root = query.Group{Op: query.OpAnd, Children: nodes}
	}

	if s.executor == nil {
		writeError(w, http.StatusServiceUnavailable, "internal_error", "query executor not configured", nil)
		return
	}

	countStart := time.Now()
	count, exact, _, durationMs, err := s.executor.Count(r.Context(), query.Request{
		From:         req.TimeRange.From.UTC(),
		To:           req.TimeRange.To.UTC(),
		Filter:       root,
		Search:       search,
		MaxTimeRange: s.maxTimeRange,
	}, countLimitGuard)
	if s.metrics != nil {
		s.metrics.QueryCountDuration.Observe(time.Since(countStart).Seconds())
	}
	if err != nil {
		s.writeQueryError(w, err)
		return
	}
	out := countResponse{
		Count:      count,
		Exact:      exact,
		DurationMs: durationMs,
	}
	if !exact {
		out.Count = countLimitGuard - 1
		out.LowerBound = true
	}
	writeJSON(w, http.StatusOK, out)
}
