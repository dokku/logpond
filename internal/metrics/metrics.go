// Package metrics owns the Prometheus registry and the full set of
// metric handles documented in PRD §13.25. Callers obtain a single
// *Metrics from New, register the additional gauges they back with
// GaugeFunc callbacks, and pass the value around through the rest of
// the wiring.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Metrics groups the named handles together so callers can pass a
// single value around instead of importing prometheus everywhere.
type Metrics struct {
	Registry *prometheus.Registry

	// Counters.
	IngestEventsTotal             *prometheus.CounterVec
	IngestSkippedLinesTotal       *prometheus.CounterVec
	ArchiveScriptInvocationsTotal *prometheus.CounterVec

	// Gauges.
	RingBufferFillRatio prometheus.GaugeFunc
	Segments            *prometheus.GaugeVec
	DiskBytes           *prometheus.GaugeVec
	Facets              *prometheus.GaugeVec
	LiveTailClients     prometheus.GaugeFunc

	// Histograms.
	QueryDuration           prometheus.Histogram
	FacetComputeDuration    prometheus.Histogram
	SearchSuggestDuration   prometheus.Histogram
	QueryCountDuration      prometheus.Histogram
	SearchBarParseDuration  prometheus.Histogram
	ArchiveScriptDuration   *prometheus.HistogramVec
}

// Options configures the metrics registration. The closure fields are
// reasonable nil-safe — a nil callback yields a gauge that reads 0.
type Options struct {
	// FillRatio reports the current ring-buffer fill in [0, 1].
	FillRatio func() float64
	// LiveTailClients reports the number of connected live-tail clients.
	LiveTailClients func() float64
}

// New builds a registry, registers process/go collectors, and creates
// every documented metric handle. The caller wires gauge backers via
// Options closures and updates the gauge-vec metrics directly.
func New(opts Options) *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	m := &Metrics{Registry: reg}

	m.IngestEventsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "logpond_ingest_events_total",
		Help: "Number of events accepted via POST /ingest/:source.",
	}, []string{"source"})
	reg.MustRegister(m.IngestEventsTotal)

	m.IngestSkippedLinesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "logpond_ingest_skipped_lines_total",
		Help: "Number of NDJSON lines skipped during ingest.",
	}, []string{"source", "reason"})
	reg.MustRegister(m.IngestSkippedLinesTotal)

	m.ArchiveScriptInvocationsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "logpond_archive_script_invocations_total",
		Help: "Number of script archive-backend invocations by mode and exit code.",
	}, []string{"mode", "exit"})
	reg.MustRegister(m.ArchiveScriptInvocationsTotal)

	fill := opts.FillRatio
	if fill == nil {
		fill = func() float64 { return 0 }
	}
	m.RingBufferFillRatio = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "logpond_ring_buffer_fill_ratio",
		Help: "Current fill ratio of the ingest ring buffer (0..1).",
	}, fill)
	reg.MustRegister(m.RingBufferFillRatio)

	m.Segments = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "logpond_segments",
		Help: "Number of segments by state.",
	}, []string{"state"})
	reg.MustRegister(m.Segments)

	m.DiskBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "logpond_disk_bytes",
		Help: "On-disk bytes consumed by segments by state.",
	}, []string{"state"})
	reg.MustRegister(m.DiskBytes)

	m.Facets = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "logpond_facets",
		Help: "Number of registered facets by kind and source.",
	}, []string{"kind", "source"})
	reg.MustRegister(m.Facets)

	clients := opts.LiveTailClients
	if clients == nil {
		clients = func() float64 { return 0 }
	}
	m.LiveTailClients = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "logpond_live_tail_clients",
		Help: "Number of connected live-tail WebSocket clients.",
	}, clients)
	reg.MustRegister(m.LiveTailClients)

	// Histogram buckets chosen to match the PRD §8.1 targets: cheap
	// operations (parse, suggest, count) cluster sub-100ms; query and
	// facet compute extend out to a few seconds.
	queryBuckets := []float64{0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5}
	fastBuckets := []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5}
	scriptBuckets := []float64{0.1, 0.5, 1, 5, 10, 30, 60, 300, 600}

	m.QueryDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "logpond_query_duration_seconds",
		Help:    "Duration of /api/query executor calls (seconds).",
		Buckets: queryBuckets,
	})
	reg.MustRegister(m.QueryDuration)

	m.FacetComputeDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "logpond_facet_compute_duration_seconds",
		Help:    "Duration of facet aggregation during query execution (seconds).",
		Buckets: queryBuckets,
	})
	reg.MustRegister(m.FacetComputeDuration)

	m.SearchSuggestDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "logpond_search_suggest_duration_seconds",
		Help:    "Duration of /api/search-suggest handler (seconds).",
		Buckets: fastBuckets,
	})
	reg.MustRegister(m.SearchSuggestDuration)

	m.QueryCountDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "logpond_query_count_duration_seconds",
		Help:    "Duration of /api/query/count handler (seconds).",
		Buckets: fastBuckets,
	})
	reg.MustRegister(m.QueryCountDuration)

	m.SearchBarParseDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "logpond_search_bar_parse_duration_seconds",
		Help:    "Duration of search-bar parser invocations (seconds).",
		Buckets: []float64{0.0001, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025},
	})
	reg.MustRegister(m.SearchBarParseDuration)

	m.ArchiveScriptDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "logpond_archive_script_duration_seconds",
		Help:    "Duration of script archive-backend invocations by mode (seconds).",
		Buckets: scriptBuckets,
	}, []string{"mode"})
	reg.MustRegister(m.ArchiveScriptDuration)

	return m
}
