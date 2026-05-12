package ui

import (
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/dokku/logpond/internal/facets"
	"github.com/dokku/logpond/internal/ingest"
	"github.com/dokku/logpond/internal/query"
)

// TestTemplates_Parse verifies the embedded template set parses cleanly
// at construction time. A missing `{{define ...}}` or a referenced
// fragment that doesn't exist would otherwise only fail at runtime when
// the bad page is requested.
func TestTemplates_Parse(t *testing.T) {
	t.Parallel()
	tmpl, err := loadTemplates(assets)
	if err != nil {
		t.Fatalf("loadTemplates: %v", err)
	}
	for _, name := range []string{
		"search.html", "tail.html", "admin.html",
		"fragments/topbar.html",
		"fragments/head.html",
		"fragments/result_list.html",
		"fragments/result_row.html",
		"fragments/facet.html",
		"fragments/suggestion_list.html",
		"fragments/count_indicator.html",
		"fragments/tail_event.html",
		"fragments/load_more.html",
		"fragments/error.html",
	} {
		if tmpl.root.Lookup(name) == nil {
			t.Errorf("template %q not registered", name)
		}
	}
}

// TestSearchPage_RendersTopBarAndForm runs the full Search-view render
// against a minimal data set and looks for known landmarks.
func TestSearchPage_RendersTopBarAndForm(t *testing.T) {
	t.Parallel()
	tmpl, err := loadTemplates(assets)
	if err != nil {
		t.Fatal(err)
	}
	body, err := tmpl.render("search.html", pageData{
		Title:   "Search",
		View:    "search",
		Archive: ArchiveInfo{Kind: "s3", Detail: "s3://bucket/prefix"},
		Page:    searchPage{},
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	html := string(body)
	for _, want := range []string{
		"<title>Search · Logpond</title>",
		`href="/static/css/app.css"`,
		`hx-post="/ui/query"`,
		`hx-post="/ui/search-suggest"`,
		`hx-post="/ui/query/count"`,
		`id="results"`,
		`Last 1 hour`,
		`<a href="/" class="active">Search</a>`, // active nav highlights search view
		`s3`, // archive pill mentions the backend kind
	} {
		if !strings.Contains(html, want) {
			t.Errorf("search.html missing %q", want)
		}
	}
}

// TestResultListFragment_RendersFacetsAndRows feeds a small dataset
// through the fragment and asserts the meaningful pieces of HTML are
// present. Snapshot-style: doesn't compare the full body, only the
// load-bearing markers each consumer relies on.
func TestResultListFragment_RendersFacetsAndRows(t *testing.T) {
	t.Parallel()
	tmpl, err := loadTemplates(assets)
	if err != nil {
		t.Fatal(err)
	}

	now := time.Date(2026, 5, 12, 14, 55, 33, 882_000_000, time.UTC)
	resp := query.Response{
		Events: []query.ResultEvent{
			{
				Timestamp: now,
				Service:   sql.NullString{String: "api", Valid: true},
				Level:     sql.NullString{String: "error", Valid: true},
				Message:   sql.NullString{String: "db connection refused", Valid: true},
				Host:      sql.NullString{String: "app-1", Valid: true},
				Source:    "dokku",
				Raw:       `{"timestamp":"2026-05-12T14:55:33.882Z"}`,
				SegmentID: "202605121400",
			},
		},
		NextCursor: "Y3Vyc29yLXRva2Vu",
		Stats:      query.Stats{RowsReturned: 1, RowsScanned: 100, SegmentsRead: 2, DurationMs: 12},
		Facets: map[string]query.FacetResult{
			"service": {
				Values:         []query.FacetValue{{Value: "api", Count: 12420}},
				CardinalityCap: 100,
				SampleSegments: 1,
			},
		},
	}

	view := resultsView{
		Events:     []eventView{toEventView(resp.Events[0])},
		NextCursor: resp.NextCursor,
		RequestB64: "cmVx",
		Stats:      resp.Stats,
		Sidebar: toFacetViews([]facets.Definition{
			{Name: "service", DisplayLabel: "Service", CardinalityCap: 100, Kind: facets.KindBuiltin},
		}, resp.Facets),
	}

	body, err := tmpl.render("fragments/result_list.html", view)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	html := string(body)
	for _, want := range []string{
		`hx-swap-oob="innerHTML:#facets"`,
		`Service`,
		`db connection refused`,
		`14:55:33.882`,
		`Load more`,
		`level-error`,
	} {
		if !strings.Contains(html, want) {
			t.Errorf("result_list missing %q", want)
		}
	}
}

func TestTailEventFragment_RendersOOBSwap(t *testing.T) {
	t.Parallel()
	tmpl, err := loadTemplates(assets)
	if err != nil {
		t.Fatal(err)
	}
	ev := tailEventFromIngest(ingest.Event{
		Timestamp: time.Date(2026, 5, 12, 15, 0, 0, 0, time.UTC),
		Service:   "api",
		Level:     "warn",
		Message:   "slow query",
		Host:      "app-1",
		Source:    "dokku",
	})
	body, err := tmpl.render("fragments/tail_event.html", ev)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	html := string(body)
	for _, want := range []string{
		`hx-swap-oob="afterbegin:#tail-events"`,
		`level-warn`,
		`slow query`,
		`15:00:00.000`,
	} {
		if !strings.Contains(html, want) {
			t.Errorf("tail_event missing %q", want)
		}
	}
}

func TestCountIndicatorFragment_FormatsLowerBound(t *testing.T) {
	t.Parallel()
	tmpl, err := loadTemplates(assets)
	if err != nil {
		t.Fatal(err)
	}
	out, err := tmpl.render("fragments/count_indicator.html", countView{Count: 10000, LowerBound: true, DurationMs: 95})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(out), "10,000+") {
		t.Errorf("count_indicator did not format lower-bound; got %s", out)
	}
}

func TestSuggestionListFragment_RendersItems(t *testing.T) {
	t.Parallel()
	tmpl, err := loadTemplates(assets)
	if err != nil {
		t.Fatal(err)
	}
	out, err := tmpl.render("fragments/suggestion_list.html", suggestionsView{Items: []suggestionView{
		{Text: "service", Kind: "core", Description: "Logical service name"},
		{Text: "level", Kind: "core", Description: "Log level"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	html := string(out)
	for _, want := range []string{`data-text="service"`, `data-text="level"`, `Logical service name`} {
		if !strings.Contains(html, want) {
			t.Errorf("suggestion_list missing %q", want)
		}
	}
}
