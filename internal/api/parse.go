package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/dokku/logpond/internal/query/parser"
)

// parseQueryRequest is the wire shape for POST /api/parse-query
// (§13.3).
type parseQueryRequest struct {
	Q string `json:"q"`
}

// parseQueryResponse mirrors the §13.3 success body. Filter and Search
// are pointers to interface so we can emit explicit `null` when the
// parser produced nothing of that kind.
type parseQueryResponse struct {
	Filter   any      `json:"filter"`
	Search   any      `json:"search"`
	Warnings []string `json:"warnings"`
}

func (s *Server) handleParseQuery(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var req parseQueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", "invalid JSON: "+err.Error(), nil)
		return
	}
	parseStart := time.Now()
	res, err := parser.Parse(req.Q)
	if s.metrics != nil {
		s.metrics.SearchBarParseDuration.Observe(time.Since(parseStart).Seconds())
	}
	if err != nil {
		var pe parser.ParseError
		if errors.As(err, &pe) {
			writeError(w, http.StatusBadRequest, "invalid_query_syntax", pe.Msg, map[string]any{"column": pe.Col})
			return
		}
		writeError(w, http.StatusBadRequest, "invalid_query_syntax", err.Error(), nil)
		return
	}
	out := parseQueryResponse{Warnings: res.Warnings}
	if res.Filter != nil {
		out.Filter = res.Filter
	}
	if res.Search != "" {
		out.Search = res.Search
	}
	writeJSON(w, http.StatusOK, out)
}
