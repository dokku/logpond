package metrics

import (
	"context"
	"log/slog"
	"runtime"
	"runtime/debug"
	"time"
)

// LiveTailShedder is implemented by the live-tail subsystem so the
// memory-pressure watchdog can shed clients without importing it.
type LiveTailShedder interface {
	// ClientCount reports the number of currently connected live-tail
	// clients.
	ClientCount() int
	// ShedOldest closes the oldest live-tail client. Returns false when
	// there are no clients to shed.
	ShedOldest() bool
	// SetAcceptingNewClients controls whether new live-tail connections
	// are accepted. Watchdog calls SetAcceptingNewClients(false) while
	// shedding and SetAcceptingNewClients(true) once memory is healthy.
	SetAcceptingNewClients(bool)
}

// WatchdogOptions configures the memory-pressure watchdog.
type WatchdogOptions struct {
	// MemoryLimit is the soft cap watched (typically runtime/debug
	// SetMemoryLimit). Zero falls back to debug.SetMemoryLimit(-1).
	MemoryLimit int64
	// Threshold is the fraction of MemoryLimit above which the watchdog
	// considers the process under pressure. Default 0.95 per PRD §8.2.
	Threshold float64
	// SustainedFor is how long the threshold must be held before
	// shedding begins. Default 30s per PRD §8.2.
	SustainedFor time.Duration
	// PollInterval is the loop tick. Default 5s.
	PollInterval time.Duration
	// LiveTail is the shedder. Required.
	LiveTail LiveTailShedder
	// Logger is used for shedding-related service log lines.
	Logger *slog.Logger
}

// Watchdog watches RSS vs GOMEMLIMIT and sheds live-tail clients when
// the process exceeds the configured threshold for the configured
// sustained period (PRD §8.2).
type Watchdog struct {
	memLimit     int64
	threshold    float64
	sustainedFor time.Duration
	pollInterval time.Duration
	liveTail     LiveTailShedder
	logger       *slog.Logger
}

// NewWatchdog constructs a Watchdog. A nil LiveTail disables shedding —
// the watchdog still runs (cheap) so callers can wire it unconditionally.
func NewWatchdog(opts WatchdogOptions) *Watchdog {
	limit := opts.MemoryLimit
	if limit <= 0 {
		limit = debug.SetMemoryLimit(-1)
	}
	thr := opts.Threshold
	if thr <= 0 {
		thr = 0.95
	}
	sus := opts.SustainedFor
	if sus <= 0 {
		sus = 30 * time.Second
	}
	poll := opts.PollInterval
	if poll <= 0 {
		poll = 5 * time.Second
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Watchdog{
		memLimit:     limit,
		threshold:    thr,
		sustainedFor: sus,
		pollInterval: poll,
		liveTail:     opts.LiveTail,
		logger:       logger,
	}
}

// Run blocks until ctx is done, polling memory at the configured cadence.
func (w *Watchdog) Run(ctx context.Context) {
	tick := time.NewTicker(w.pollInterval)
	defer tick.Stop()
	var overSince time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			w.tick(&overSince, time.Now())
		}
	}
}

// tick is the unit of work executed each poll. Exposed for tests.
func (w *Watchdog) tick(overSince *time.Time, now time.Time) {
	if w.memLimit <= 0 || w.memLimit == int64(^uint64(0)>>1) {
		// No usable cap (debug.SetMemoryLimit returns math.MaxInt64
		// when unset). Skip — watchdog cannot make a relative decision.
		return
	}
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	used := int64(ms.HeapInuse + ms.StackInuse + ms.MSpanInuse + ms.MCacheInuse)
	ratio := float64(used) / float64(w.memLimit)

	if ratio < w.threshold {
		if !overSince.IsZero() {
			w.logger.Info("memory pressure relieved",
				"used_bytes", used, "limit_bytes", w.memLimit, "ratio", ratio)
			*overSince = time.Time{}
			if w.liveTail != nil {
				w.liveTail.SetAcceptingNewClients(true)
			}
		}
		return
	}

	if overSince.IsZero() {
		*overSince = now
		w.logger.Warn("memory pressure detected",
			"used_bytes", used, "limit_bytes", w.memLimit, "ratio", ratio,
			"sustained_required", w.sustainedFor.String())
		return
	}
	if now.Sub(*overSince) < w.sustainedFor {
		return
	}
	if w.liveTail == nil {
		return
	}
	w.liveTail.SetAcceptingNewClients(false)
	if w.liveTail.ClientCount() == 0 {
		return
	}
	if w.liveTail.ShedOldest() {
		w.logger.Warn("memory pressure: shed live-tail client",
			"used_bytes", used, "limit_bytes", w.memLimit, "ratio", ratio,
			"remaining_clients", w.liveTail.ClientCount())
	}
}
