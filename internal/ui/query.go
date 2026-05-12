package ui

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/dokku/logpond/internal/facets"
	"github.com/dokku/logpond/internal/query"
	"github.com/dokku/logpond/internal/query/parser"
)

// queryFormInput is the wire shape of the HTMX-submitted search form.
// HTMX serializes the form as application/x-www-form-urlencoded so we
// pull fields off r.PostForm rather than decoding JSON.
type queryFormInput struct {
	Q           string
	TimePreset  string
	From        string
	To          string
	Sort        string
	Limit       int
	Cursor      string
	FacetClicks []facetSelection // composed from data attributes by Alpine
}

type facetSelection struct {
	Field  string
	Values []string
}

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		s.renderError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	in := readQueryForm(r)
	req, err := s.buildQueryRequest(in, 50)
	if err != nil {
		s.renderError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	resp, err := s.executor.Run(r.Context(), req)
	if err != nil {
		s.renderQueryError(w, err)
		return
	}
	view := s.buildResultsViewQ(resp, req, in.Q)
	s.renderHTML(w, "fragments/result_list.html", view, http.StatusOK)
}

func (s *Server) handleLoadMore(w http.ResponseWriter, r *http.Request) {
	cursor := r.URL.Query().Get("cursor")
	reqB64 := r.URL.Query().Get("request")
	if cursor == "" || reqB64 == "" {
		s.renderError(w, http.StatusBadRequest, "bad_request", "missing cursor or request")
		return
	}
	raw, err := base64.URLEncoding.DecodeString(reqB64)
	if err != nil {
		s.renderError(w, http.StatusBadRequest, "bad_request", "invalid request token")
		return
	}
	var saved savedRequest
	if err := json.Unmarshal(raw, &saved); err != nil {
		s.renderError(w, http.StatusBadRequest, "bad_request", "corrupt request token")
		return
	}
	req := saved.toRequest()
	req.Cursor = cursor
	resp, err := s.executor.Run(r.Context(), req)
	if err != nil {
		s.renderQueryError(w, err)
		return
	}
	view := s.buildResultsViewQ(resp, req, saved.Q)
	// Load-more replaces the button-row, so we emit only the rows + a new
	// load-more button. Use a smaller fragment.
	s.renderHTML(w, "fragments/load_more.html", view, http.StatusOK)
}

// buildResultsView packages the executor output for the result_list
// fragment, including the base64-encoded request that load-more sends
// back to continue pagination.
func (s *Server) buildResultsViewQ(resp query.Response, req query.Request, q string) resultsView {
	view := s.buildResultsView(resp, req)
	if view.NextCursor != "" {
		saved := savedFromRequest(req)
		saved.Q = q
		raw, _ := json.Marshal(saved)
		view.RequestB64 = base64.URLEncoding.EncodeToString(raw)
	}
	return view
}

func (s *Server) buildResultsView(resp query.Response, req query.Request) resultsView {
	events := make([]eventView, 0, len(resp.Events))
	for _, ev := range resp.Events {
		events = append(events, toEventView(ev))
	}
	var defs []facets.Definition
	if s.facets != nil {
		defs = s.facets.List()
	}
	sidebar := toFacetViews(defs, resp.Facets)
	out := resultsView{
		Events:     events,
		NextCursor: resp.NextCursor,
		Stats:      resp.Stats,
		Sidebar:    sidebar,
	}
	if resp.NextCursor != "" {
		saved := savedFromRequest(req)
		raw, _ := json.Marshal(saved)
		out.RequestB64 = base64.URLEncoding.EncodeToString(raw)
	}
	return out
}

// savedRequest is the persistable form of query.Request the load-more
// link round-trips. It carries enough to rebuild the request on the
// next click; not all fields need to survive (facets, for instance,
// are recomputed each page).
type savedRequest struct {
	From   time.Time       `json:"from"`
	To     time.Time       `json:"to"`
	Q      string          `json:"q,omitempty"`
	Search string          `json:"search,omitempty"`
	Sort   []query.SortKey `json:"sort,omitempty"`
	Limit  int             `json:"limit,omitempty"`
}

func savedFromRequest(r query.Request) savedRequest {
	out := savedRequest{From: r.From, To: r.To, Search: r.Search, Sort: r.Sort, Limit: r.Limit}
	// We don't preserve the filter tree directly; we re-parse from `q`
	// on each load-more. The handler reconstructs Q from req.Search for
	// load-more requests issued from a tree-only filter; that branch is
	// not user-reachable in v1 since the UI submits via Q only.
	return out
}

func (sr savedRequest) toRequest() query.Request {
	out := query.Request{
		From:   sr.From,
		To:     sr.To,
		Search: sr.Search,
		Sort:   sr.Sort,
		Limit:  sr.Limit,
	}
	if sr.Q != "" {
		pres, err := parser.Parse(sr.Q)
		if err == nil {
			out.Filter = pres.Filter
			if pres.Search != "" {
				if out.Search != "" {
					out.Search = out.Search + " " + pres.Search
				} else {
					out.Search = pres.Search
				}
			}
		}
	}
	return out
}

func readQueryForm(r *http.Request) queryFormInput {
	in := queryFormInput{
		Q:          strings.TrimSpace(r.PostFormValue("q")),
		TimePreset: r.PostFormValue("time_preset"),
		From:       r.PostFormValue("from"),
		To:         r.PostFormValue("to"),
		Sort:       r.PostFormValue("sort"),
		Cursor:     r.PostFormValue("cursor"),
	}
	if v, err := strconv.Atoi(r.PostFormValue("limit")); err == nil {
		in.Limit = v
	}
	return in
}

// buildQueryRequest converts the form values into a query.Request,
// resolving the time-range preset to absolute bounds and parsing `q`
// into a filter tree. defaultLimit applies when limit isn't set.
func (s *Server) buildQueryRequest(in queryFormInput, defaultLimit int) (query.Request, error) {
	from, to, err := resolveTimeRange(in.TimePreset, in.From, in.To)
	if err != nil {
		return query.Request{}, err
	}
	limit := in.Limit
	if limit <= 0 {
		limit = defaultLimit
	}
	req := query.Request{
		From:            from,
		To:              to,
		Limit:           limit,
		Cursor:          in.Cursor,
		MaxTimeRange:    time.Duration(s.maxTimeRange) * time.Second,
		FacetSampleSize: s.facetSampleSize,
	}
	if in.Q != "" {
		pres, perr := parser.Parse(in.Q)
		if perr != nil {
			return query.Request{}, fmt.Errorf("query syntax: %w", perr)
		}
		if pres.Filter != nil {
			req.Filter = pres.Filter
		}
		req.Search = pres.Search
	}
	if s.facets != nil {
		for _, d := range s.facets.List() {
			req.Facets = append(req.Facets, query.FacetSpec{
				Name: d.Name, Field: d.Field, CardinalityCap: d.CardinalityCap,
			})
		}
	}
	return req, nil
}

// resolveTimeRange maps the form preset into absolute (from, to). A
// "custom" preset reads From/To as RFC3339 or browser-native
// `datetime-local` strings; anything else is parsed as a fixed window
// ending "now".
func resolveTimeRange(preset, fromS, toS string) (time.Time, time.Time, error) {
	now := time.Now().UTC()
	switch preset {
	case "15m":
		return now.Add(-15 * time.Minute), now, nil
	case "1h", "":
		return now.Add(-time.Hour), now, nil
	case "6h":
		return now.Add(-6 * time.Hour), now, nil
	case "24h":
		return now.Add(-24 * time.Hour), now, nil
	case "7d":
		return now.Add(-7 * 24 * time.Hour), now, nil
	case "custom":
		from, ferr := parseLocalOrRFC3339(fromS)
		if ferr != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("invalid from: %w", ferr)
		}
		to, terr := parseLocalOrRFC3339(toS)
		if terr != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("invalid to: %w", terr)
		}
		return from, to, nil
	}
	return now.Add(-time.Hour), now, nil
}

func parseLocalOrRFC3339(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, errors.New("empty")
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t.UTC(), nil
	}
	// HTML datetime-local emits YYYY-MM-DDTHH:MM(:SS) without timezone.
	for _, layout := range []string{"2006-01-02T15:04:05", "2006-01-02T15:04"} {
		if t, err := time.ParseInLocation(layout, s, time.UTC); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("unrecognised time format %q", s)
}

func (s *Server) renderQueryError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, query.ErrInvalidTimeRange):
		s.renderError(w, http.StatusBadRequest, "invalid_time_range", err.Error())
	case errors.Is(err, query.ErrTimeRangeTooLarge):
		s.renderError(w, http.StatusBadRequest, "time_range_too_large", err.Error())
	case errors.Is(err, query.ErrCursorInvalidated):
		s.renderError(w, http.StatusGone, "cursor_invalidated", err.Error())
	case query.IsFilterTooDeep(err):
		s.renderError(w, http.StatusBadRequest, "filter_too_deep", err.Error())
	case errors.Is(err, query.ErrInvalidFilter):
		s.renderError(w, http.StatusBadRequest, "invalid_filter", err.Error())
	default:
		s.logger.Error("ui query failed", "err", err)
		s.renderError(w, http.StatusInternalServerError, "internal_error", err.Error())
	}
}

func (s *Server) renderError(w http.ResponseWriter, status int, code, msg string) {
	body, err := s.tmpl.render("fragments/error.html", map[string]any{"Code": code, "Message": msg})
	if err != nil {
		http.Error(w, msg, status)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}
