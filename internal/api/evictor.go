package api

import (
	"context"
	"time"

	"github.com/dokku/logpond/internal/catalog"
	"github.com/dokku/logpond/internal/query"
)

// Evictor enforces the rehydrated-segment TTL described in PRD §7.10.
// Every interval it scans rehydrated rows with evict_after < now and
// removes them. Busy segments are skipped and retried next pass.
type Evictor struct {
	catalog  *catalog.Catalog
	executor *query.Executor
	logger   interface {
		Warn(msg string, args ...any)
		Info(msg string, args ...any)
	}
	now func() time.Time
}

// EvictorOptions configures an Evictor.
type EvictorOptions struct {
	Catalog  *catalog.Catalog
	Executor *query.Executor
	Logger   interface {
		Warn(msg string, args ...any)
		Info(msg string, args ...any)
	}
	Now func() time.Time
}

// NewEvictor constructs an Evictor.
func NewEvictor(opts EvictorOptions) *Evictor {
	if opts.Now == nil {
		opts.Now = func() time.Time { return time.Now().UTC() }
	}
	return &Evictor{
		catalog:  opts.Catalog,
		executor: opts.Executor,
		logger:   opts.Logger,
		now:      opts.Now,
	}
}

// Run blocks until ctx ends, sweeping the rehydrated set every interval.
func (e *Evictor) Run(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			_, _ = e.Sweep(ctx)
		}
	}
}

// Sweep evicts every expired rehydrated segment. Returns the count
// evicted and the first error encountered (the loop continues past
// per-segment errors so a transient failure doesn't block the rest).
func (e *Evictor) Sweep(ctx context.Context) (int, error) {
	segs, err := e.catalog.ListRehydratedSegments(ctx)
	if err != nil {
		return 0, err
	}
	now := e.now().UTC()
	evicted := 0
	var firstErr error
	for _, seg := range segs {
		if !seg.EvictAfter.Valid {
			// Persistent rehydrated rows: leave alone.
			continue
		}
		if seg.EvictAfter.Time.After(now) {
			continue
		}
		if e.executor != nil && e.executor.IsSegmentBusy(seg.ID) {
			if e.logger != nil {
				e.logger.Warn("evictor: skipping busy segment", "id", seg.ID)
			}
			continue
		}
		if err := evictRehydratedSegment(ctx, e.catalog, seg); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			if e.logger != nil {
				e.logger.Warn("evictor: evict failed", "id", seg.ID, "err", err)
			}
			continue
		}
		if e.logger != nil {
			e.logger.Info("evictor: evicted", "id", seg.ID)
		}
		evicted++
	}
	return evicted, firstErr
}
