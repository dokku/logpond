package metrics

import (
	"context"
	"time"

	"github.com/dokku/logpond/internal/catalog"
	"github.com/dokku/logpond/internal/facets"
)

// Refresher periodically reads segment and facet state and updates the
// corresponding gauges. The active loop is started by Run; close ctx to
// stop it. The first refresh runs synchronously before Run blocks so
// /metrics is populated immediately after startup.
type Refresher struct {
	M       *Metrics
	Catalog *catalog.Catalog
	Facets  *facets.Registry
}

// Refresh reads current state once and updates the gauges.
func (r Refresher) Refresh(ctx context.Context) {
	if r.M == nil {
		return
	}
	if r.Catalog != nil {
		segs, err := r.Catalog.ListSegments(ctx)
		if err == nil {
			counts := map[string]int{}
			bytes := map[string]int64{}
			for _, s := range segs {
				counts[s.State]++
				size := s.SizeBytes
				if s.SizeCompressed.Valid && s.SizeCompressed.Int64 > 0 {
					size = s.SizeCompressed.Int64
				}
				bytes[s.State] += size
			}
			r.M.Segments.Reset()
			r.M.DiskBytes.Reset()
			for state, n := range counts {
				r.M.Segments.WithLabelValues(state).Set(float64(n))
			}
			for state, b := range bytes {
				r.M.DiskBytes.WithLabelValues(state).Set(float64(b))
			}
		}
	}
	if r.Facets != nil {
		r.M.Facets.Reset()
		buckets := map[[2]string]int{}
		for _, d := range r.Facets.List() {
			buckets[[2]string{string(d.Kind), string(d.Source)}]++
		}
		for k, v := range buckets {
			r.M.Facets.WithLabelValues(k[0], k[1]).Set(float64(v))
		}
	}
}

// Run refreshes once and then on interval until ctx is done.
func (r Refresher) Run(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 15 * time.Second
	}
	r.Refresh(ctx)
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			r.Refresh(ctx)
		}
	}
}
