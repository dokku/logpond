package ui

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/dokku/logpond/internal/facets"
	"github.com/dokku/logpond/internal/query"
)

func (s *Server) handleSuggest(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		s.renderError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	q := r.FormValue("q")
	cursorPos := len(q)
	if v, err := strconv.Atoi(r.FormValue("cursor_pos")); err == nil && v >= 0 && v <= len(q) {
		cursorPos = v
	}

	ctxQ := classifySuggestContext(q, cursorPos)
	max := 20

	var items []suggestionView
	switch ctxQ.kind {
	case sctxField:
		items = s.fieldSuggestions(ctxQ.partial, max)
	case sctxCombinator:
		items = combinatorSuggestions(ctxQ.partial, max)
	case sctxValue:
		items = s.valueSuggestions(r.Context(), ctxQ.field, ctxQ.partial, max)
	}

	view := suggestionsView{Items: items}
	s.renderHTML(w, "fragments/suggestion_list.html", view, http.StatusOK)
}

// suggestContextKind classifies the cursor position for autocomplete. A
// trimmed-down peer of the api package's classifier (kept separate so
// the UI can evolve its surface without rippling into the JSON API).
type suggestContextKind int

const (
	sctxField suggestContextKind = iota
	sctxValue
	sctxCombinator
)

type suggestContext struct {
	kind    suggestContextKind
	field   string
	partial string
}

func classifySuggestContext(q string, pos int) suggestContext {
	if pos > len(q) {
		pos = len(q)
	}
	prefix := q[:pos]
	i := len(prefix) - 1
	for i >= 0 && isIdentChar(prefix[i]) {
		i--
	}
	partial := prefix[i+1:]
	var prev byte
	if i >= 0 {
		prev = prefix[i]
	}
	switch prev {
	case ':':
		field := readIdentBackward(prefix, i)
		if i-len(field)-1 >= 0 && prefix[i-len(field)-1] == '@' {
			return suggestContext{kind: sctxValue, field: "attributes." + field, partial: partial}
		}
		return suggestContext{kind: sctxValue, field: field, partial: partial}
	case '@':
		return suggestContext{kind: sctxField, partial: "@" + partial}
	case ' ', '\t', 0:
		if partial == "" && lastTokenEndsPredicate(prefix) {
			return suggestContext{kind: sctxCombinator, partial: partial}
		}
		return suggestContext{kind: sctxField, partial: partial}
	}
	return suggestContext{kind: sctxField, partial: partial}
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

func lastTokenEndsPredicate(s string) bool {
	trimmed := strings.TrimRight(s, " \t")
	if trimmed == "" {
		return false
	}
	last := trimmed[len(trimmed)-1]
	if last == ')' {
		return true
	}
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

func (s *Server) fieldSuggestions(partial string, max int) []suggestionView {
	prefix := strings.ToLower(strings.TrimPrefix(partial, "@"))
	type entry struct {
		text, desc, kind string
	}
	candidates := []entry{
		{"service", "Logical service name", "core"},
		{"level", "Log level", "core"},
		{"host", "Hostname", "core"},
		{"source", "Configured ingest source", "core"},
		{"message", "Log message", "core"},
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
			candidates = append(candidates, entry{text, d.DisplayLabel, "custom"})
		}
	}
	out := make([]suggestionView, 0, max)
	for _, e := range candidates {
		t := strings.ToLower(strings.TrimPrefix(e.text, "@"))
		if prefix != "" && !strings.HasPrefix(t, prefix) {
			continue
		}
		out = append(out, suggestionView{Text: e.text, Kind: e.kind, Description: e.desc})
		if len(out) >= max {
			break
		}
	}
	return out
}

func combinatorSuggestions(partial string, max int) []suggestionView {
	upper := strings.ToUpper(partial)
	candidates := []string{"AND", "OR", "NOT"}
	out := make([]suggestionView, 0, len(candidates))
	for _, c := range candidates {
		if upper != "" && !strings.HasPrefix(c, upper) {
			continue
		}
		out = append(out, suggestionView{Text: c, Kind: "combinator"})
		if len(out) >= max {
			break
		}
	}
	return out
}

func (s *Server) valueSuggestions(ctx context.Context, field, partial string, max int) []suggestionView {
	if s.executor == nil || field == "" {
		return nil
	}
	now := time.Now().UTC()
	req := query.Request{
		From:            now.Add(-time.Hour),
		To:              now,
		MaxTimeRange:    time.Duration(s.maxTimeRange) * time.Second,
		FacetSampleSize: s.facetSampleSize,
		Facets:          []query.FacetSpec{{Name: "_suggest", Field: field, CardinalityCap: max + 1}},
		Limit:           1,
	}
	resp, err := s.executor.Run(ctx, req)
	if err != nil {
		return nil
	}
	fr, ok := resp.Facets["_suggest"]
	if !ok {
		return nil
	}
	out := make([]suggestionView, 0, max)
	lower := strings.ToLower(partial)
	for _, v := range fr.Values {
		if lower != "" && !strings.Contains(strings.ToLower(v.Value), lower) {
			continue
		}
		out = append(out, suggestionView{Text: v.Value, Kind: "value", Count: v.Count, HasCount: true})
		if len(out) >= max {
			break
		}
	}
	return out
}
