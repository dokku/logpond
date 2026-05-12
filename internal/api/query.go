package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/dokku/logpond/internal/query"
)

// queryRequest is the wire shape of POST /api/query (§13.4). The
// handler accepts Form A (`q`), Form B (`filter`), and Form C
// (`filters`); only one may be set.
type queryRequest struct {
	TimeRange *timeRange        `json:"time_range"`
	Q         string            `json:"q,omitempty"`
	Filter    json.RawMessage   `json:"filter,omitempty"`
	Filters   []json.RawMessage `json:"filters,omitempty"`
	Search    string            `json:"search,omitempty"`
	Facets    []string          `json:"facets,omitempty"`
	Sort      []query.SortKey   `json:"sort,omitempty"`
	Limit     int               `json:"limit,omitempty"`
	Cursor    string            `json:"cursor,omitempty"`
}

type timeRange struct {
	From time.Time `json:"from"`
	To   time.Time `json:"to"`
}

type queryResponse struct {
	Events     []eventDTO     `json:"events"`
	NextCursor string         `json:"next_cursor,omitempty"`
	Stats      statsDTO       `json:"stats"`
	Facets     map[string]any `json:"facets"`
}

type eventDTO struct {
	Timestamp  string         `json:"timestamp"`
	Service    *string        `json:"service"`
	Level      *string        `json:"level"`
	Message    *string        `json:"message"`
	Host       *string        `json:"host"`
	Source     string         `json:"source"`
	Attributes map[string]any `json:"attributes"`
	Raw        string         `json:"raw"`
}

type statsDTO struct {
	RowsScanned  int64 `json:"rows_scanned"`
	RowsReturned int64 `json:"rows_returned"`
	SegmentsRead int   `json:"segments_read"`
	DurationMs   int64 `json:"duration_ms"`
}

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
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
	if req.Q != "" {
		writeError(w, http.StatusNotImplemented, "bad_request", "search-bar parsing (Form A) lands in Phase 5", nil)
		return
	}

	if s.executor == nil {
		writeError(w, http.StatusServiceUnavailable, "internal_error", "query executor not configured", nil)
		return
	}

	var root query.Node
	if len(req.Filter) > 0 {
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

	resp, err := s.executor.Run(r.Context(), query.Request{
		From:         req.TimeRange.From.UTC(),
		To:           req.TimeRange.To.UTC(),
		Filter:       root,
		Search:       req.Search,
		Sort:         req.Sort,
		Limit:        req.Limit,
		Cursor:       req.Cursor,
		MaxTimeRange: s.maxTimeRange,
	})
	if err != nil {
		s.writeQueryError(w, err)
		return
	}

	out := queryResponse{
		Events:     make([]eventDTO, 0, len(resp.Events)),
		NextCursor: resp.NextCursor,
		Stats: statsDTO{
			RowsScanned:  resp.Stats.RowsScanned,
			RowsReturned: resp.Stats.RowsReturned,
			SegmentsRead: resp.Stats.SegmentsRead,
			DurationMs:   resp.Stats.DurationMs,
		},
		Facets: map[string]any{},
	}
	for _, ev := range resp.Events {
		out.Events = append(out.Events, toEventDTO(ev))
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) writeQueryError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, query.ErrInvalidTimeRange):
		writeError(w, http.StatusBadRequest, "invalid_time_range", err.Error(), nil)
	case errors.Is(err, query.ErrTimeRangeTooLarge):
		writeError(w, http.StatusBadRequest, "time_range_too_large", err.Error(), nil)
	case errors.Is(err, query.ErrCursorInvalidated):
		writeError(w, http.StatusGone, "cursor_invalidated", err.Error(), nil)
	case query.IsFilterTooDeep(err):
		writeError(w, http.StatusBadRequest, "filter_too_deep", err.Error(), nil)
	case errors.Is(err, query.ErrInvalidFilter):
		writeError(w, http.StatusBadRequest, "invalid_filter", err.Error(), nil)
	case errors.Is(err, query.ErrInvalidSort):
		writeError(w, http.StatusBadRequest, "bad_request", err.Error(), nil)
	default:
		s.logger.Error("query failed", "err", err)
		writeError(w, http.StatusInternalServerError, "internal_error", err.Error(), nil)
	}
}

func toEventDTO(ev query.ResultEvent) eventDTO {
	dto := eventDTO{
		Timestamp: ev.Timestamp.UTC().Format(time.RFC3339Nano),
		Source:    ev.Source,
		Raw:       ev.Raw,
	}
	if ev.Service.Valid {
		v := ev.Service.String
		dto.Service = &v
	}
	if ev.Level.Valid {
		v := ev.Level.String
		dto.Level = &v
	}
	if ev.Message.Valid {
		v := ev.Message.String
		dto.Message = &v
	}
	if ev.Host.Valid {
		v := ev.Host.String
		dto.Host = &v
	}
	if ev.Attributes != "" {
		var attrs map[string]any
		if err := json.Unmarshal([]byte(ev.Attributes), &attrs); err == nil {
			dto.Attributes = attrs
		}
	}
	if dto.Attributes == nil {
		dto.Attributes = map[string]any{}
	}
	return dto
}
