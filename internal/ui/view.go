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

// adminPage is the body of the Admin view (PRD §14.4).
type adminPage struct {
	Retention   retentionView
	Archive     archiveView
	Facets      facetTableView
	Storage     storageView
	Segments    segmentsTableView
	SampleSize  int
	FacetFields []string // candidate fields for the Add-facet modal
}

type retentionView struct {
	MaxAge              string
	MaxSize             string
	ArchiveBeforeDelete bool
	LastResult          *retentionResultView
}

// retentionResultView shapes a single retention pass for the result
// fragment.
type retentionResultView struct {
	DryRun    bool
	Evaluated int
	Actions   []retentionActionView
	Error     string
}

type retentionActionView struct {
	SegmentID string
	Action    string
	Reason    string
	Executed  bool
	Error     string
}

// archiveView combines static config-derived data and live probe state
// so the admin pane can render both S3 and script variants.
type archiveView struct {
	Kind         string // "s3", "script", "none"
	Detail       string
	S3           *archiveS3View
	Script       *archiveScriptView
	Capabilities archiveCapsView
	LastVerify   *verifyResultView
	LastTest     *testInvocationView
}

type archiveS3View struct {
	Endpoint string
	Bucket   string
	Prefix   string
}

type archiveScriptView struct {
	Path    string
	Timeout string
}

type archiveCapsView struct {
	Archive  string // "yes" | "no" | "unknown"
	Retrieve string
	Verify   string
}

type verifyResultView struct {
	Backend           string
	Scanned           int
	Verified          int
	Orphaned          []string
	Dangling          []string
	Missing           []string
	Failed            []verifyFailedView
	Error             string
}

type verifyFailedView struct {
	SegmentID string
	ExitCode  int
	Note      string
}

type testInvocationView struct {
	Capabilities archiveCapsView
	Error        string
	Stdout       string
	Stderr       string
}

// facetTableView wraps the list with a "kind grouping" hint the
// template uses for editable affordances.
type facetTableView struct {
	Items []facetRowView
}

type facetRowView struct {
	Name           string
	Field          string
	DisplayLabel   string
	CardinalityCap int
	ValueType      string
	Kind           string // "builtin" or "custom"
	Source         string // "" | "config" | "ui"
	Editable       bool   // ui-source facets are fully editable
	CapEditable    bool   // builtin facets can edit cap only
}

// storageView is rendered for both the initial page and the 10s
// polling fragment.
type storageView struct {
	Active     storageBucket
	Sealed     storageBucket
	Rehydrated storageBucket
	Archived   storageBucket
	TotalLocal storageBucket
	MaxSize    string
	UpdatedAt  string
}

type storageBucket struct {
	Count       int
	SizeBytes   int64
	SizeDisplay string
}

// segmentsTableView shapes the paginated segments list.
type segmentsTableView struct {
	Items   []segmentRowView
	State   string // active filter
	Offset  int
	Limit   int
	Total   int
	HasPrev bool
	HasNext bool
	PrevURL string
	NextURL string
	States  []string
}

type segmentRowView struct {
	ID         string
	State      string
	StateLabel string
	TimeStart  string
	TimeEnd    string
	TimeRange  string
	Rows       int64
	RowsDisplay string
	SizeBytes  int64
	SizeDisplay string
	Sources    []string
	IsCurrent  bool

	// Action shapes the per-row affordance the template renders.
	Action segmentActionView
}

type segmentActionView struct {
	Kind     string // "archive" | "rehydrate" | "evict" | "job" | "none"
	JobID    string
	Tooltip  string
	Disabled bool
}

// jobRowView is the small fragment used to replace a segment row's
// action cell when a job is running.
type jobRowView struct {
	SegmentID string
	JobID     string
	State     string
	Progress  string
	Done      bool
	Failed    bool
	Error     string
}

// importResultView is rendered into the import modal on success/failure.
type importResultView struct {
	OK         bool
	SegmentID  string
	State      string
	Rows       int64
	SizeBytes  int64
	Overlaps   []importOverlapView
	EvictAfter string
	Persistent bool
	Error      string
}

type importOverlapView struct {
	SegmentID string
	TimeRange string
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
