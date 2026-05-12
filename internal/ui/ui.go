// Package ui serves Logpond's browser-facing surface: the three views
// (Search, Live tail, Admin), the /ui/* HTML-fragment endpoints that
// HTMX targets, and the embedded static asset bundle. See PRD §11
// (Frontend stack) and §14 (UI sketches).
package ui

import (
	"embed"
	"io/fs"
	"log/slog"
	"net/http"

	"github.com/dokku/logpond/internal/facets"
	"github.com/dokku/logpond/internal/jobs"
	"github.com/dokku/logpond/internal/query"
)

//go:embed all:templates all:static
var assets embed.FS

// Server wires the UI views to the same executor + registries the JSON
// API uses. Each /ui/* handler delegates to those engines and renders
// the result via templates rather than JSON.
type Server struct {
	logger          *slog.Logger
	executor        *query.Executor
	facets          *facets.Registry
	jobs            *jobs.Manager
	tmpl            *templates
	staticHandler   http.Handler
	maxTimeRange    int64 // seconds
	facetSampleSize int

	// archiveInfo is rendered into the top bar status pill. The string
	// is set at startup; live status (last-success time) is out of scope
	// for v1.
	archiveInfo   ArchiveInfo
	retentionInfo RetentionInfo
}

// ArchiveInfo describes the configured archive backend for display in
// the admin and top-bar surfaces.
type ArchiveInfo struct {
	Kind   string // "none", "s3", "script"
	Detail string // human-readable summary (bucket, script path, etc.)
}

// Options bundles construction parameters.
type Options struct {
	Logger          *slog.Logger
	Executor        *query.Executor
	Facets          *facets.Registry
	Jobs            *jobs.Manager
	MaxTimeRange    int64
	FacetSampleSize int
	Archive         ArchiveInfo
	Retention       RetentionInfo
	LiveTail        http.Handler
}

// RetentionInfo carries the retention policy summary the admin view
// renders. Populated from config at startup; not live state.
type RetentionInfo struct {
	MaxAge              string
	MaxSize             string
	ArchiveBeforeDelete bool
}

// New parses templates and prepares the static handler.
func New(opts Options) (*Server, error) {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	tmpl, err := loadTemplates(assets)
	if err != nil {
		return nil, err
	}
	staticFS, err := fs.Sub(assets, "static")
	if err != nil {
		return nil, err
	}
	return &Server{
		logger:          opts.Logger,
		executor:        opts.Executor,
		facets:          opts.Facets,
		jobs:            opts.Jobs,
		tmpl:            tmpl,
		staticHandler:   newStaticHandler(staticFS, opts.Logger),
		maxTimeRange:    opts.MaxTimeRange,
		facetSampleSize: opts.FacetSampleSize,
		archiveInfo:     opts.Archive,
		retentionInfo:   opts.Retention,
	}, nil
}

// Register wires the UI routes onto the supplied mux. The mux is the
// same chi router the JSON API uses, so /ui/* paths sit beside /api/*.
func (s *Server) Register(r interface {
	Get(pattern string, h http.HandlerFunc)
	Post(pattern string, h http.HandlerFunc)
	Handle(pattern string, h http.Handler)
}) {
	r.Get("/", s.handleSearchPage)
	r.Get("/tail", s.handleTailPage)
	r.Get("/admin", s.handleAdminPage)

	r.Post("/ui/query", s.handleQuery)
	r.Post("/ui/query/count", s.handleCount)
	r.Post("/ui/search-suggest", s.handleSuggest)
	r.Get("/ui/load-more", s.handleLoadMore)

	r.Handle("/static/*", http.StripPrefix("/static/", s.staticHandler))
}
