// Package api hosts Logpond's HTTP surface: ingest, query, admin, and
// the live-tail WebSocket. Phase 2 introduces just the chi router,
// healthcheck, and the ingest handler; later phases register their
// routes against the same Server.
package api

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/dokku/logpond/internal/archive"
	"github.com/dokku/logpond/internal/catalog"
	"github.com/dokku/logpond/internal/facets"
	"github.com/dokku/logpond/internal/ingest"
	"github.com/dokku/logpond/internal/jobs"
	"github.com/dokku/logpond/internal/metrics"
	"github.com/dokku/logpond/internal/query"
	"github.com/dokku/logpond/internal/retention"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

// maxDecompressedBody is the cap on the size of a single ingest body
// after gzip decoding (PRD §7.1). Compressed inputs that decode to more
// than this many bytes return 413.
const maxDecompressedBody = 32 << 20 // 32MB

// Server wires extractors, the ring buffer, and the metrics handles to
// the chi router.
type Server struct {
	router          chi.Router
	logger          *slog.Logger
	buffer          *ingest.Buffer
	metrics         *Metrics
	extractors      map[string]*ingest.Extractor
	executor        *query.Executor
	facets          *facets.Registry
	retention       *retention.Evaluator
	catalog         *catalog.Catalog
	archiveBackend  archive.Backend
	jobs            *jobs.Manager
	maxTimeRange    time.Duration
	facetSampleSize int
	dataDir         string
	rehydrationTTL  time.Duration
}

// Metrics is the subset of the central metrics struct that the API
// package needs. Keeps the package free of a hard dependency on the
// full prometheus surface.
type Metrics = metrics.Metrics

// Options bundles construction parameters.
type Options struct {
	Logger          *slog.Logger
	Buffer          *ingest.Buffer
	Metrics         *Metrics
	Extractors      map[string]*ingest.Extractor
	Executor        *query.Executor
	Facets          *facets.Registry
	Retention       *retention.Evaluator
	Catalog         *catalog.Catalog
	ArchiveBackend  archive.Backend
	Jobs            *jobs.Manager
	MaxTimeRange    time.Duration
	FacetSampleSize int
	DataDir         string
	RehydrationTTL  time.Duration
}

// New builds a Server with all currently-implemented routes registered.
func New(opts Options) *Server {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	r := chi.NewRouter()
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)

	s := &Server{
		router:          r,
		logger:          opts.Logger,
		buffer:          opts.Buffer,
		metrics:         opts.Metrics,
		extractors:      opts.Extractors,
		executor:        opts.Executor,
		facets:          opts.Facets,
		retention:       opts.Retention,
		catalog:         opts.Catalog,
		archiveBackend:  opts.ArchiveBackend,
		jobs:            opts.Jobs,
		maxTimeRange:    opts.MaxTimeRange,
		facetSampleSize: opts.FacetSampleSize,
		dataDir:         opts.DataDir,
		rehydrationTTL:  opts.RehydrationTTL,
	}

	r.Get("/healthz", s.handleHealthz)
	r.Post("/ingest/{source_name}", s.handleIngest)
	r.Post("/api/query", s.handleQuery)
	r.Post("/api/query/count", s.handleQueryCount)
	r.Post("/api/parse-query", s.handleParseQuery)
	r.Post("/api/search-suggest", s.handleSearchSuggest)
	r.Get("/api/facets", s.handleListFacets)
	r.Post("/api/facets", s.handleCreateFacet)
	r.Patch("/api/facets/{name}", s.handlePatchFacet)
	r.Delete("/api/facets/{name}", s.handleDeleteFacet)
	r.Post("/api/admin/retention/run", s.handleRetentionRun)
	r.Post("/api/archive", s.handleArchive)
	r.Post("/api/admin/archive/verify", s.handleArchiveVerify)
	r.Get("/api/admin/archive/capabilities", s.handleArchiveCapabilities)
	r.Post("/api/rehydrate", s.handleRehydrate)
	r.Delete("/api/rehydrated/{id}", s.handleDeleteRehydrated)
	r.Post("/api/import", s.handleImport)
	r.Get("/api/jobs/{id}", s.handleGetJob)

	return s
}

// Handler returns the http.Handler. Use this to mount the server in
// an http.Server.
func (s *Server) Handler() http.Handler { return s.router }

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	sourceName := chi.URLParam(r, "source_name")
	ex, ok := s.extractors[sourceName]
	if !ok {
		writeError(w, http.StatusNotFound, "unknown_source", fmt.Sprintf("source %q is not configured", sourceName), nil)
		return
	}

	if !acceptableContentType(r.Header.Get("Content-Type")) {
		writeError(w, http.StatusUnsupportedMediaType, "bad_request",
			"Content-Type must be application/x-ndjson or application/json", nil)
		return
	}

	body, err := s.readBody(r)
	if err != nil {
		if errors.Is(err, errPayloadTooLarge) {
			writeError(w, http.StatusRequestEntityTooLarge, "payload_too_large",
				fmt.Sprintf("decoded body exceeds %d bytes", maxDecompressedBody), nil)
			return
		}
		if errors.Is(err, errUnsupportedEncoding) {
			writeError(w, http.StatusUnsupportedMediaType, "bad_request",
				"only Content-Encoding: gzip is supported", nil)
			return
		}
		writeError(w, http.StatusBadRequest, "bad_request", err.Error(), nil)
		return
	}

	accepted, skipped, events := s.parseAndExtract(sourceName, ex, body)

	if err := s.buffer.Append(events); err != nil {
		if errors.Is(err, ingest.ErrBufferFull) {
			w.Header().Set("Retry-After", "1")
			writeError(w, http.StatusTooManyRequests, "buffer_full",
				"ingest buffer is full; retry after backoff", nil)
			return
		}
		writeError(w, http.StatusInternalServerError, "internal_error", err.Error(), nil)
		return
	}

	if s.metrics != nil {
		if accepted > 0 {
			s.metrics.IngestEventsTotal.WithLabelValues(sourceName).Add(float64(accepted))
		}
		if skipped > 0 {
			s.metrics.IngestSkippedLinesTotal.WithLabelValues(sourceName, "parse_error").Add(float64(skipped))
		}
	}

	writeJSON(w, http.StatusAccepted, map[string]int{"accepted": accepted, "skipped": skipped})
}

func (s *Server) parseAndExtract(source string, ex *ingest.Extractor, body []byte) (int, int, []ingest.Event) {
	events := make([]ingest.Event, 0)
	accepted, skipped := 0, 0

	for _, line := range splitLines(body) {
		if len(line) == 0 {
			// PRD §7.1: empty lines are silently skipped and don't count.
			continue
		}
		ev, err := ex.Extract(line)
		if err != nil {
			skipped++
			s.logger.Warn("ingest line skipped", "source", source, "err", err)
			continue
		}
		events = append(events, ev)
		accepted++
	}
	return accepted, skipped, events
}

var (
	errPayloadTooLarge     = errors.New("payload too large")
	errUnsupportedEncoding = errors.New("unsupported encoding")
)

func (s *Server) readBody(r *http.Request) ([]byte, error) {
	defer r.Body.Close()

	var src io.Reader = r.Body
	switch enc := strings.TrimSpace(r.Header.Get("Content-Encoding")); enc {
	case "", "identity":
		// no-op
	case "gzip":
		gz, err := gzip.NewReader(r.Body)
		if err != nil {
			return nil, fmt.Errorf("decoding gzip: %w", err)
		}
		defer gz.Close()
		src = gz
	default:
		return nil, errUnsupportedEncoding
	}

	// +1 lets us distinguish "exactly at the cap" from "over the cap".
	limited := io.LimitReader(src, maxDecompressedBody+1)
	buf, err := io.ReadAll(limited)
	if err != nil {
		return nil, fmt.Errorf("reading body: %w", err)
	}
	if int64(len(buf)) > maxDecompressedBody {
		return nil, errPayloadTooLarge
	}
	return buf, nil
}

// splitLines splits an NDJSON body on `\n`, also handling `\r\n`
// terminators. Trailing empty segments are returned as zero-length
// slices so the caller can apply its skip-empty-line policy uniformly.
func splitLines(body []byte) [][]byte {
	out := make([][]byte, 0, 8)
	start := 0
	for i := 0; i < len(body); i++ {
		if body[i] != '\n' {
			continue
		}
		end := i
		if end > start && body[end-1] == '\r' {
			end--
		}
		out = append(out, body[start:end])
		start = i + 1
	}
	if start < len(body) {
		out = append(out, body[start:])
	}
	return out
}

func acceptableContentType(ct string) bool {
	if ct == "" {
		return true
	}
	// Strip parameters such as "; charset=utf-8".
	if i := strings.IndexByte(ct, ';'); i >= 0 {
		ct = ct[:i]
	}
	switch strings.ToLower(strings.TrimSpace(ct)) {
	case "application/x-ndjson", "application/json", "application/ndjson":
		return true
	}
	return false
}

// errorEnvelope mirrors PRD §13.1's uniform error envelope.
type errorEnvelope struct {
	Error errorBody `json:"error"`
}

type errorBody struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}

func writeError(w http.ResponseWriter, status int, code, message string, details map[string]any) {
	writeJSON(w, status, errorEnvelope{Error: errorBody{Code: code, Message: message, Details: details}})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
