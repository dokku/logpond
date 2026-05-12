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
	Events     []eventDTO          `json:"events"`
	NextCursor string              `json:"next_cursor,omitempty"`
	Stats      statsDTO            `json:"stats"`
	Facets     map[string]facetDTO `json:"facets"`
}

type facetDTO struct {
	Values         []facetValueDTO `json:"values"`
	CardinalityCap int             `json:"cardinality_cap"`
	Truncated      bool            `json:"truncated"`
	TruncatedCount int             `json:"truncated_count,omitempty"`
	SampleSegments int             `json:"sample_segments"`
}

type facetValueDTO struct {
	Value string `json:"value"`
	Count int64  `json:"count"`
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

	specs, unknown := s.resolveFacetSpecs(req.Facets)
	if len(unknown) > 0 {
		writeError(w, http.StatusBadRequest, "bad_request",
			fmt.Sprintf("unknown facets: %v", unknown), map[string]any{"unknown": unknown})
		return
	}

	queryStart := time.Now()
	resp, err := s.executor.Run(r.Context(), query.Request{
		From:            req.TimeRange.From.UTC(),
		To:              req.TimeRange.To.UTC(),
		Filter:          root,
		Search:          search,
		Sort:            req.Sort,
		Limit:           req.Limit,
		Cursor:          req.Cursor,
		MaxTimeRange:    s.maxTimeRange,
		Facets:          specs,
		FacetSampleSize: s.facetSampleSize,
	})
	if s.metrics != nil {
		s.metrics.QueryDuration.Observe(time.Since(queryStart).Seconds())
		if len(specs) > 0 && resp.Stats.FacetDurationMs > 0 {
			s.metrics.FacetComputeDuration.Observe(float64(resp.Stats.FacetDurationMs) / 1000.0)
		}
	}
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
		Facets: map[string]facetDTO{},
	}
	for _, ev := range resp.Events {
		out.Events = append(out.Events, toEventDTO(ev))
	}
	for name, fr := range resp.Facets {
		out.Facets[name] = toFacetDTO(fr)
	}
	writeJSON(w, http.StatusOK, out)
}

// resolveFacetSpecs maps facet names into executor-ready specs using
// the registry. An empty `requested` list expands to "all facets"
// (§13.4: the response includes all configured facets when the request
// omits `facets`). Unknown names are returned for a 400 reply.
func (s *Server) resolveFacetSpecs(requested []string) ([]query.FacetSpec, []string) {
	if s.facets == nil {
		return nil, nil
	}
	if len(requested) == 0 {
		all := s.facets.List()
		out := make([]query.FacetSpec, 0, len(all))
		for _, d := range all {
			out = append(out, query.FacetSpec{
				Name: d.Name, Field: d.Field, CardinalityCap: d.CardinalityCap,
			})
		}
		return out, nil
	}
	out := make([]query.FacetSpec, 0, len(requested))
	var unknown []string
	for _, name := range requested {
		d, ok := s.facets.Get(name)
		if !ok {
			unknown = append(unknown, name)
			continue
		}
		out = append(out, query.FacetSpec{
			Name: d.Name, Field: d.Field, CardinalityCap: d.CardinalityCap,
		})
	}
	return out, unknown
}

func toFacetDTO(fr query.FacetResult) facetDTO {
	values := make([]facetValueDTO, 0, len(fr.Values))
	for _, v := range fr.Values {
		values = append(values, facetValueDTO{Value: v.Value, Count: v.Count})
	}
	return facetDTO{
		Values:         values,
		CardinalityCap: fr.CardinalityCap,
		Truncated:      fr.Truncated,
		TruncatedCount: fr.TruncatedCount,
		SampleSegments: fr.SampleSegments,
	}
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
