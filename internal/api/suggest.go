package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/dokku/logpond/internal/facets"
	"github.com/dokku/logpond/internal/query"
)

// suggestRequest is the wire shape of POST /api/search-suggest (§13.6).
type suggestRequest struct {
	Q         string     `json:"q"`
	CursorPos int        `json:"cursor_pos"`
	TimeRange *timeRange `json:"time_range"`
	Max       int        `json:"max"`
}

type suggestResponse struct {
	Context     string           `json:"context"`
	Field       string           `json:"field,omitempty"`
	Prefix      string           `json:"prefix,omitempty"`
	Suggestions []suggestionItem `json:"suggestions"`
}

type suggestionItem struct {
	Text        string `json:"text"`
	Kind        string `json:"kind"`
	Count       int64  `json:"count,omitempty"`
	Description string `json:"description,omitempty"`
}

func (s *Server) handleSearchSuggest(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	start := time.Now()
	defer func() {
		if s.metrics != nil {
			s.metrics.SearchSuggestDuration.Observe(time.Since(start).Seconds())
		}
	}()
	var req suggestRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", "invalid JSON: "+err.Error(), nil)
		return
	}
	if req.Max <= 0 || req.Max > 50 {
		req.Max = 20
	}
	if req.CursorPos < 0 {
		req.CursorPos = 0
	}
	if req.CursorPos > len(req.Q) {
		req.CursorPos = len(req.Q)
	}

	ctxQ := classifyContext(req.Q, req.CursorPos)
	resp := suggestResponse{
		Context:     string(ctxQ.kind),
		Field:       ctxQ.field,
		Prefix:      ctxQ.partial,
		Suggestions: []suggestionItem{},
	}
	switch ctxQ.kind {
	case ctxField:
		resp.Suggestions = s.fieldSuggestions(ctxQ.partial, req.Max)
	case ctxCombinator:
		resp.Suggestions = combinatorSuggestions(ctxQ.partial, req.Max)
	case ctxValue:
		resp.Suggestions = s.valueSuggestions(r.Context(), ctxQ.field, ctxQ.partial, req.TimeRange, req.Max)
	default:
		resp.Suggestions = s.fieldSuggestions(ctxQ.partial, req.Max)
		resp.Context = string(ctxField)
	}
	writeJSON(w, http.StatusOK, resp)
}

// contextKind enumerates the contexts §7.5 / §13.6 distinguish between.
type contextKind string

const (
	ctxField      contextKind = "field"
	ctxValue      contextKind = "value"
	ctxCombinator contextKind = "combinator"
)

// cursorContext describes what the cursor is positioned inside.
type cursorContext struct {
	kind    contextKind
	field   string // populated for value context (raw field, e.g. "service" or "attributes.user.id")
	partial string // partial token under the cursor
}

// classifyContext is a small state machine that walks the query prefix
// and emits the suggestion context. It's a simpler relative of the full
// lexer in internal/query/parser; we tolerate ambiguity in favour of
// returning *some* useful suggestion even for malformed inputs.
func classifyContext(q string, pos int) cursorContext {
	if pos > len(q) {
		pos = len(q)
	}
	prefix := q[:pos]
	i := len(prefix) - 1
	// Walk back over the partial token under the cursor.
	for i >= 0 && isIdentChar(prefix[i]) {
		i--
	}
	partial := prefix[i+1:]
	// Examine the char immediately preceding the partial token.
	var prev byte
	if i >= 0 {
		prev = prefix[i]
	}
	switch prev {
	case ':':
		field := readIdentBackward(prefix, i)
		// Did the field name follow a `@`? If so, treat it as an attribute path.
		if i-len(field)-1 >= 0 && prefix[i-len(field)-1] == '@' {
			return cursorContext{kind: ctxValue, field: "attributes." + field, partial: partial}
		}
		return cursorContext{kind: ctxValue, field: field, partial: partial}
	case '(':
		// Inside a value list — find the field bound to the opening paren.
		field := findEnclosingField(prefix, i)
		if field != "" {
			return cursorContext{kind: ctxValue, field: field, partial: partial}
		}
		// Otherwise this is a parenthesised subexpression — suggest fields.
		return cursorContext{kind: ctxField, partial: partial}
	case ',':
		field := findEnclosingField(prefix, i)
		if field != "" {
			return cursorContext{kind: ctxValue, field: field, partial: partial}
		}
		return cursorContext{kind: ctxField, partial: partial}
	case '@':
		return cursorContext{kind: ctxField, partial: "@" + partial}
	case ' ', '\t', 0:
		// After whitespace (or at start): if the partial is empty and the
		// preceding non-whitespace tail looks like a complete predicate,
		// suggest combinators. Otherwise field.
		if partial == "" && lastTokenEndsPredicate(prefix) {
			return cursorContext{kind: ctxCombinator, partial: partial}
		}
		return cursorContext{kind: ctxField, partial: partial}
	}
	return cursorContext{kind: ctxField, partial: partial}
}

func isIdentChar(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '_' || b == '.' || b == '-'
}

func readIdentBackward(s string, end int) string {
	i := end - 1
	for i >= 0 && isIdentChar(s[i]) {
		i--
	}
	return s[i+1 : end]
}

// findEnclosingField scans back from a comma or open-paren to the
// `field:(` that opened the value list, returning the field name. Empty
// means "no enclosing list found".
func findEnclosingField(s string, openIdx int) string {
	// Walk back through the prefix looking for a matching opening paren.
	depth := 0
	i := openIdx
	// If we started on `,`, find the open paren first.
	if i < len(s) && s[i] == ',' {
		for i > 0 {
			i--
			c := s[i]
			if c == ')' {
				depth++
			}
			if c == '(' {
				if depth == 0 {
					break
				}
				depth--
			}
		}
	}
	if i < 0 || s[i] != '(' {
		return ""
	}
	if i == 0 || s[i-1] != ':' {
		return ""
	}
	return readIdentBackward(s, i-1)
}

// lastTokenEndsPredicate reports whether the trailing token in s looks
// like a value (e.g. `level:error` rather than a partial field name).
func lastTokenEndsPredicate(s string) bool {
	trimmed := strings.TrimRight(s, " \t")
	if trimmed == "" {
		return false
	}
	last := trimmed[len(trimmed)-1]
	if last == ')' {
		return true
	}
	// Look for a `:` somewhere in the trailing token.
	for i := len(trimmed) - 1; i >= 0; i-- {
		c := trimmed[i]
		if c == ' ' || c == '\t' {
			return false
		}
		if c == ':' {
			return true
		}
	}
	return false
}

func (s *Server) fieldSuggestions(partial string, max int) []suggestionItem {
	prefix := strings.TrimPrefix(partial, "@")
	prefix = strings.ToLower(prefix)
	type entry struct {
		text, desc, kind string
	}
	var entries []entry
	// Core fields (always available).
	cores := []struct {
		name, desc string
	}{
		{"service", "Logical service name"},
		{"level", "Log level"},
		{"host", "Hostname"},
		{"source", "Configured ingest source"},
		{"message", "Log message"},
	}
	for _, c := range cores {
		entries = append(entries, entry{c.name, c.desc, "core"})
	}
	if s.facets != nil {
		for _, d := range s.facets.List() {
			if d.Kind == facets.KindBuiltin {
				continue
			}
			text := d.Name
			if strings.HasPrefix(d.Field, "attributes.") {
				text = "@" + d.Field[len("attributes."):]
			}
			entries = append(entries, entry{text, d.DisplayLabel, "custom"})
		}
	}
	out := make([]suggestionItem, 0, max)
	for _, e := range entries {
		t := strings.ToLower(strings.TrimPrefix(e.text, "@"))
		if prefix != "" && !strings.HasPrefix(t, prefix) {
			continue
		}
		out = append(out, suggestionItem{Text: e.text, Kind: e.kind, Description: e.desc})
		if len(out) >= max {
			break
		}
	}
	return out
}

func combinatorSuggestions(partial string, max int) []suggestionItem {
	candidates := []string{"AND", "OR", "NOT"}
	out := make([]suggestionItem, 0, len(candidates))
	upper := strings.ToUpper(partial)
	for _, c := range candidates {
		if upper != "" && !strings.HasPrefix(c, upper) {
			continue
		}
		out = append(out, suggestionItem{Text: c, Kind: "combinator"})
		if len(out) >= max {
			break
		}
	}
	return out
}

func (s *Server) valueSuggestions(ctx context.Context, field, partial string, tr *timeRange, max int) []suggestionItem {
	if s.executor == nil || field == "" {
		return []suggestionItem{}
	}
	// Default to the last hour when no time_range is supplied; the UI
	// commonly omits time_range on the very first keystroke.
	from, to := suggestTimeRange(tr)
	spec := query.FacetSpec{Name: "_suggest", Field: field, CardinalityCap: max + 1}
	req := query.Request{
		From:            from,
		To:              to,
		MaxTimeRange:    s.maxTimeRange,
		Facets:          []query.FacetSpec{spec},
		FacetSampleSize: s.facetSampleSize,
		Limit:           1,
	}
	resp, err := s.executor.Run(ctx, req)
	if err != nil {
		return []suggestionItem{}
	}
	fr, ok := resp.Facets["_suggest"]
	if !ok {
		return []suggestionItem{}
	}
	out := make([]suggestionItem, 0, max)
	lower := strings.ToLower(partial)
	for _, v := range fr.Values {
		if lower != "" && !strings.Contains(strings.ToLower(v.Value), lower) {
			continue
		}
		out = append(out, suggestionItem{Text: v.Value, Kind: "value", Count: v.Count})
		if len(out) >= max {
			break
		}
	}
	return out
}

func suggestTimeRange(tr *timeRange) (time.Time, time.Time) {
	now := time.Now().UTC()
	if tr == nil || tr.From.IsZero() || tr.To.IsZero() {
		return now.Add(-time.Hour), now
	}
	return tr.From.UTC(), tr.To.UTC()
}
