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
	"sync"
	"time"

	"github.com/dokku/logpond/internal/archive"
	"github.com/dokku/logpond/internal/catalog"
	"github.com/dokku/logpond/internal/facets"
	"github.com/dokku/logpond/internal/jobs"
	"github.com/dokku/logpond/internal/query"
	"github.com/dokku/logpond/internal/retention"
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
	catalog         *catalog.Catalog
	retention       *retention.Evaluator
	archiveBackend  archive.Backend
	tmpl            *templates
	staticHandler   http.Handler
	maxTimeRange    int64 // seconds
	facetSampleSize int

	// dataDir is required by import / segment handlers to stage uploads.
	dataDir        string
	rehydrationTTL time.Duration

	// archiveInfo is rendered into the top bar status pill. The string
	// is set at startup; live status (last-success time) is out of scope
	// for v1.
	archiveInfo   ArchiveInfo
	retentionInfo RetentionInfo

	// adminState carries the most-recent retention/verify/test results
	// so the admin view can re-render them after a partial-page refresh.
	adminMu    sync.Mutex
	adminState adminLiveState
}

// adminLiveState is the in-memory record of the most recent admin
// operations. Survives a page refresh inside the same process; resets
// on restart.
type adminLiveState struct {
	lastRetention *retentionResultView
	lastVerify    *verifyResultView
	lastTest      *testInvocationView
}

// ArchiveInfo describes the configured archive backend for display in
// the admin and top-bar surfaces.
type ArchiveInfo struct {
	Kind     string // "none", "s3", "script"
	Detail   string // human-readable summary (bucket, script path, etc.)
	Endpoint string // S3 only
	Bucket   string // S3 only
	Prefix   string // S3 only
	Path     string // script only
	Timeout  string // script only
}

// Options bundles construction parameters.
type Options struct {
	Logger          *slog.Logger
	Executor        *query.Executor
	Facets          *facets.Registry
	Jobs            *jobs.Manager
	Catalog         *catalog.Catalog
	Retention       *retention.Evaluator
	ArchiveBackend  archive.Backend
	MaxTimeRange    int64
	FacetSampleSize int
	DataDir         string
	RehydrationTTL  time.Duration
	Archive         ArchiveInfo
	RetentionInfo   RetentionInfo
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
		catalog:         opts.Catalog,
		retention:       opts.Retention,
		archiveBackend:  opts.ArchiveBackend,
		tmpl:            tmpl,
		staticHandler:   newStaticHandler(staticFS, opts.Logger),
		maxTimeRange:    opts.MaxTimeRange,
		facetSampleSize: opts.FacetSampleSize,
		dataDir:         opts.DataDir,
		rehydrationTTL:  opts.RehydrationTTL,
		archiveInfo:     opts.Archive,
		retentionInfo:   opts.RetentionInfo,
	}, nil
}

// Register wires the UI routes onto the supplied mux. The mux is the
// same chi router the JSON API uses, so /ui/* paths sit beside /api/*.
func (s *Server) Register(r interface {
	Get(pattern string, h http.HandlerFunc)
	Post(pattern string, h http.HandlerFunc)
	Patch(pattern string, h http.HandlerFunc)
	Delete(pattern string, h http.HandlerFunc)
	Handle(pattern string, h http.Handler)
}) {
	r.Get("/", s.handleSearchPage)
	r.Get("/tail", s.handleTailPage)
	r.Get("/admin", s.handleAdminPage)

	r.Post("/ui/query", s.handleQuery)
	r.Post("/ui/query/count", s.handleCount)
	r.Post("/ui/search-suggest", s.handleSuggest)
	r.Get("/ui/load-more", s.handleLoadMore)

	r.Post("/ui/admin/retention/run", s.handleAdminRetentionRun)
	r.Post("/ui/admin/archive/verify", s.handleAdminArchiveVerify)
	r.Post("/ui/admin/archive/test", s.handleAdminArchiveTest)
	r.Get("/ui/admin/facets", s.handleAdminFacetsTable)
	r.Post("/ui/admin/facets", s.handleAdminFacetCreate)
	r.Patch("/ui/admin/facets/{name}", s.handleAdminFacetPatch)
	r.Delete("/ui/admin/facets/{name}", s.handleAdminFacetDelete)
	r.Get("/ui/admin/storage", s.handleAdminStorage)
	r.Get("/ui/admin/segments", s.handleAdminSegments)
	r.Post("/ui/admin/segments/{id}/archive", s.handleAdminSegmentArchive)
	r.Post("/ui/admin/segments/{id}/rehydrate", s.handleAdminSegmentRehydrate)
	r.Delete("/ui/admin/rehydrated/{id}", s.handleAdminRehydratedDelete)
	r.Get("/ui/admin/jobs/{id}", s.handleAdminJob)
	r.Post("/ui/admin/import", s.handleAdminImport)

	r.Handle("/static/*", http.StripPrefix("/static/", s.staticHandler))
}
