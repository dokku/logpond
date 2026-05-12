package ui

import (
	"encoding/json"
	"time"

	"github.com/dokku/logpond/internal/facets"
	"github.com/dokku/logpond/internal/query"
)

// pageData is the top-level template data shared by every full-page
// render (layout, top bar, page body).
type pageData struct {
	Title   string
	View    string
	Archive ArchiveInfo
	Page    any
}

// searchPage is the body of the Search view.
type searchPage struct {
	// Filled when a query was already submitted via traditional form
	// (no-JS fallback). Empty on first GET.
	InitialResults *resultsView
}

// adminPage is the body of the Admin view stub.
type adminPage struct {
	Retention retentionView
	Archive   ArchiveInfo
	Facets    []facets.Definition
}

type retentionView struct {
	MaxAge              string
	MaxSize             string
	ArchiveBeforeDelete bool
}

// resultsView shapes /ui/query output for fragments/result_list.html.
type resultsView struct {
	Events     []eventView
	NextCursor string
	RequestB64 string
	Sidebar    []facetView
	Stats      query.Stats
}

// eventView is a flat, template-friendly projection of query.ResultEvent.
type eventView struct {
	Timestamp        time.Time
	TimestampDisplay string
	TimestampISO     string
	Service          string
	HasService       bool
	Level            string
	HasLevel         bool
	LevelClass       string
	LevelLabel       string
	Message          string
	MessagePreview   string
	HasMessage       bool
	Host             string
	HasHost          bool
	Source           string
	Raw              string
	Attributes       map[string]any
}

// facetView shapes a single facet for the sidebar.
type facetView struct {
	Name           string
	Label          string
	Values         []facetValueView
	Truncated      bool
	TruncatedCount int
	SampleSegments int
}

type facetValueView struct {
	Value string
	Count int64
}

// suggestionsView is the body of /ui/search-suggest.
type suggestionsView struct {
	Items []suggestionView
}

type suggestionView struct {
	Text        string
	Kind        string
	Description string
	Count       int64
	HasCount    bool
}

// countView shapes the live count indicator response.
type countView struct {
	Count      int64
	Display    string
	LowerBound bool
	DurationMs int64
}

// tailEventView is the body of one event row in the live tail.
type tailEventView = eventView

func toEventView(ev query.ResultEvent) eventView {
	v := eventView{
		Timestamp:        ev.Timestamp,
		TimestampDisplay: ev.Timestamp.UTC().Format("15:04:05.000"),
		TimestampISO:     ev.Timestamp.UTC().Format(time.RFC3339Nano),
		Source:           ev.Source,
		Raw:              ev.Raw,
	}
	if ev.Service.Valid {
		v.Service = ev.Service.String
		v.HasService = true
	}
	if ev.Level.Valid {
		v.Level = ev.Level.String
		v.HasLevel = true
		v.LevelClass = levelClass(&ev.Level.String)
		v.LevelLabel = levelLabel(&ev.Level.String)
	}
	if ev.Message.Valid {
		v.Message = ev.Message.String
		v.MessagePreview = shorten(ev.Message.String, 240)
		v.HasMessage = true
	}
	if ev.Host.Valid {
		v.Host = ev.Host.String
		v.HasHost = true
	}
	if ev.Attributes != "" {
		var m map[string]any
		if err := json.Unmarshal([]byte(ev.Attributes), &m); err == nil {
			v.Attributes = m
		}
	}
	if v.Attributes == nil {
		v.Attributes = map[string]any{}
	}
	return v
}

func toFacetViews(defs []facets.Definition, results map[string]query.FacetResult) []facetView {
	out := make([]facetView, 0, len(defs))
	for _, d := range defs {
		fr, ok := results[d.Name]
		if !ok {
			continue
		}
		fv := facetView{
			Name:           d.Name,
			Label:          d.DisplayLabel,
			Truncated:      fr.Truncated,
			TruncatedCount: fr.TruncatedCount,
			SampleSegments: fr.SampleSegments,
		}
		for _, v := range fr.Values {
			fv.Values = append(fv.Values, facetValueView{Value: v.Value, Count: v.Count})
		}
		out = append(out, fv)
	}
	return out
}
