// Package metrics owns the Prometheus registry and the full set of
// metric handles documented in PRD §13.25. Phase 2 registers only the
// ingest-side counters and the ring-buffer gauge; later phases will
// register query, archive, and live-tail metrics into the same
// registry.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Metrics groups the named handles together so callers can pass a
// single value around instead of importing prometheus everywhere.
type Metrics struct {
	Registry *prometheus.Registry

	IngestEventsTotal       *prometheus.CounterVec
	IngestSkippedLinesTotal *prometheus.CounterVec
	RingBufferFillRatio     prometheus.GaugeFunc
}

// Options configures the metrics registration. FillRatio is a closure
// returning the current ring-buffer fill in [0, 1]; pass nil if the
// caller hasn't built the buffer yet (the gauge will read 0).
type Options struct {
	FillRatio func() float64
}

// New builds a registry, registers process/go collectors, and creates
// the ingest metric handles. Subsequent phases extend the returned
// Metrics value with new handles.
func New(opts Options) *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	ingestEvents := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "logpond_ingest_events_total",
		Help: "Number of events accepted via POST /ingest/:source.",
	}, []string{"source"})
	reg.MustRegister(ingestEvents)

	ingestSkipped := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "logpond_ingest_skipped_lines_total",
		Help: "Number of NDJSON lines skipped during ingest.",
	}, []string{"source", "reason"})
	reg.MustRegister(ingestSkipped)

	fill := opts.FillRatio
	if fill == nil {
		fill = func() float64 { return 0 }
	}
	ringFill := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "logpond_ring_buffer_fill_ratio",
		Help: "Current fill ratio of the ingest ring buffer (0..1).",
	}, fill)
	reg.MustRegister(ringFill)

	return &Metrics{
		Registry:                reg,
		IngestEventsTotal:       ingestEvents,
		IngestSkippedLinesTotal: ingestSkipped,
		RingBufferFillRatio:     ringFill,
	}
}
