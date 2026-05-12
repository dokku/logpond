// Command loadgen drives a Logpond instance with sustained and/or burst
// NDJSON ingest. It targets the §8.1 performance envelope: 1,000 events/sec
// sustained and 5,000 events/sec bursts of up to 10 seconds. Output is a
// summary of accepted, skipped, 429s, errors, and request-latency percentiles.
//
// Example: 7-day soak at 1k events/sec into the default source:
//
//	go run ./cmd/loadgen \
//	  -url http://localhost:8080 \
//	  -source default \
//	  -rate 1000 -duration 168h
//
// Burst then sustained:
//
//	go run ./cmd/loadgen \
//	  -url http://localhost:8080 \
//	  -burst-rate 5000 -burst-duration 10s \
//	  -rate 1000 -duration 60s
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type flags struct {
	URL           string
	Source        string
	Rate          float64
	Duration      time.Duration
	BurstRate     float64
	BurstDuration time.Duration
	BatchSize     int
	Concurrency   int
	MessageBytes  int
	Services      int
	Levels        string
	ReportEvery   time.Duration
	Seed          uint64
	HTTPTimeout   time.Duration
}

func parseFlags() flags {
	var f flags
	flag.StringVar(&f.URL, "url", "http://localhost:8080", "Logpond base URL")
	flag.StringVar(&f.Source, "source", "default", "ingest source name (must match config sources[].name)")
	flag.Float64Var(&f.Rate, "rate", 1000, "sustained events/sec (0 disables sustained phase)")
	flag.DurationVar(&f.Duration, "duration", 60*time.Second, "sustained phase duration")
	flag.Float64Var(&f.BurstRate, "burst-rate", 0, "burst events/sec (0 disables burst phase)")
	flag.DurationVar(&f.BurstDuration, "burst-duration", 10*time.Second, "burst phase duration")
	flag.IntVar(&f.BatchSize, "batch-size", 50, "events per ingest request")
	flag.IntVar(&f.Concurrency, "concurrency", 8, "concurrent HTTP workers")
	flag.IntVar(&f.MessageBytes, "message-bytes", 120, "approximate size of the synthetic message field")
	flag.IntVar(&f.Services, "services", 8, "distinct synthetic service names")
	flag.StringVar(&f.Levels, "levels", "info,info,info,warn,error,debug", "comma-separated level pool (repeats bias the distribution)")
	flag.DurationVar(&f.ReportEvery, "report-every", 10*time.Second, "interval for progress lines (0 disables)")
	flag.Uint64Var(&f.Seed, "seed", 0, "RNG seed (0 = wall-clock)")
	flag.DurationVar(&f.HTTPTimeout, "http-timeout", 30*time.Second, "per-request HTTP timeout")
	flag.Parse()
	return f
}

func main() {
	cfg := parseFlags()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	client := &http.Client{Timeout: cfg.HTTPTimeout}
	defer client.CloseIdleConnections()

	seed := cfg.Seed
	if seed == 0 {
		seed = uint64(time.Now().UnixNano())
	}

	gen := newEventGenerator(seed, cfg.MessageBytes, cfg.Services, cfg.Levels)

	totals := &stats{}
	if cfg.BurstRate > 0 {
		fmt.Fprintf(os.Stdout, "phase=burst rate=%.0f duration=%s\n", cfg.BurstRate, cfg.BurstDuration)
		run(ctx, client, cfg, cfg.BurstRate, cfg.BurstDuration, gen, totals, "burst")
	}
	if cfg.Rate > 0 && ctx.Err() == nil {
		fmt.Fprintf(os.Stdout, "phase=sustained rate=%.0f duration=%s\n", cfg.Rate, cfg.Duration)
		run(ctx, client, cfg, cfg.Rate, cfg.Duration, gen, totals, "sustained")
	}

	totals.report(os.Stdout, "total")
}

func run(ctx context.Context, client *http.Client, cfg flags, rate float64, dur time.Duration, gen *eventGenerator, totals *stats, phase string) {
	phaseCtx, cancel := context.WithTimeout(ctx, dur)
	defer cancel()

	phaseStats := &stats{}
	batches := make(chan []byte, cfg.Concurrency*4)

	var wg sync.WaitGroup
	for i := 0; i < cfg.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker(phaseCtx, client, cfg, batches, phaseStats, totals)
		}()
	}

	tickEvery := time.Duration(float64(time.Second) * float64(cfg.BatchSize) / rate)
	if tickEvery <= 0 {
		tickEvery = time.Microsecond
	}

	stop := make(chan struct{})
	if cfg.ReportEvery > 0 {
		go progress(phaseCtx, cfg.ReportEvery, phase, phaseStats, stop)
	}

	ticker := time.NewTicker(tickEvery)
	defer ticker.Stop()

loop:
	for {
		select {
		case <-phaseCtx.Done():
			break loop
		case <-ticker.C:
			body := gen.batch(cfg.BatchSize)
			select {
			case batches <- body:
			case <-phaseCtx.Done():
				break loop
			}
		}
	}

	close(batches)
	wg.Wait()
	close(stop)

	phaseStats.report(os.Stdout, phase)
}

func worker(ctx context.Context, client *http.Client, cfg flags, batches <-chan []byte, phase, totals *stats) {
	url := cfg.URL + "/ingest/" + cfg.Source
	for body := range batches {
		send(ctx, client, url, body, phase, totals)
	}
}

func send(ctx context.Context, client *http.Client, url string, body []byte, phase, totals *stats) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		phase.recordError()
		totals.recordError()
		return
	}
	req.Header.Set("Content-Type", "application/x-ndjson")

	start := time.Now()
	resp, err := client.Do(req)
	latency := time.Since(start)

	if err != nil {
		phase.recordError()
		totals.recordError()
		phase.recordLatency(latency)
		totals.recordLatency(latency)
		return
	}
	defer resp.Body.Close()

	phase.recordLatency(latency)
	totals.recordLatency(latency)

	switch resp.StatusCode {
	case http.StatusAccepted:
		var r struct {
			Accepted int `json:"accepted"`
			Skipped  int `json:"skipped"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&r)
		phase.recordAccepted(r.Accepted, r.Skipped)
		totals.recordAccepted(r.Accepted, r.Skipped)
	case http.StatusTooManyRequests:
		_, _ = io.Copy(io.Discard, resp.Body)
		phase.recordBackpressure()
		totals.recordBackpressure()
	default:
		_, _ = io.Copy(io.Discard, resp.Body)
		phase.recordError()
		totals.recordError()
	}
}

func progress(ctx context.Context, every time.Duration, phase string, s *stats, stop <-chan struct{}) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-stop:
			return
		case <-t.C:
			s.report(os.Stdout, phase+":progress")
		}
	}
}

type stats struct {
	accepted     atomic.Int64
	skipped      atomic.Int64
	backpressure atomic.Int64
	errors       atomic.Int64
	requests     atomic.Int64
	startNanos   atomic.Int64

	latencyMu sync.Mutex
	latencies []time.Duration
}

func (s *stats) recordAccepted(n, skipped int) {
	s.accepted.Add(int64(n))
	s.skipped.Add(int64(skipped))
	s.requests.Add(1)
	s.markStart()
}

func (s *stats) recordBackpressure() {
	s.backpressure.Add(1)
	s.requests.Add(1)
	s.markStart()
}

func (s *stats) recordError() {
	s.errors.Add(1)
	s.requests.Add(1)
	s.markStart()
}

func (s *stats) recordLatency(d time.Duration) {
	s.latencyMu.Lock()
	defer s.latencyMu.Unlock()
	s.latencies = append(s.latencies, d)
}

func (s *stats) markStart() {
	now := time.Now().UnixNano()
	s.startNanos.CompareAndSwap(0, now)
}

func (s *stats) report(w io.Writer, label string) {
	accepted := s.accepted.Load()
	skipped := s.skipped.Load()
	backpressure := s.backpressure.Load()
	errs := s.errors.Load()
	reqs := s.requests.Load()

	s.latencyMu.Lock()
	lats := append([]time.Duration(nil), s.latencies...)
	s.latencyMu.Unlock()

	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })

	startNanos := s.startNanos.Load()
	var rate float64
	if startNanos > 0 {
		elapsed := time.Since(time.Unix(0, startNanos)).Seconds()
		if elapsed > 0 {
			rate = float64(accepted) / elapsed
		}
	}

	p50 := pct(lats, 0.50)
	p95 := pct(lats, 0.95)
	p99 := pct(lats, 0.99)

	fmt.Fprintf(w, "%s accepted=%d skipped=%d backpressure_429=%d errors=%d requests=%d rate_eps=%.1f p50=%s p95=%s p99=%s\n",
		label, accepted, skipped, backpressure, errs, reqs, rate,
		p50, p95, p99,
	)
}

func pct(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}

type eventGenerator struct {
	mu        sync.Mutex
	rng       *rand.Rand
	services  []string
	levels    []string
	msgFiller []byte
	counter   uint64
	hostname  string
}

func newEventGenerator(seed uint64, msgBytes, nServices int, levels string) *eventGenerator {
	r := rand.New(rand.NewPCG(seed, seed^0x9E3779B97F4A7C15))
	if nServices < 1 {
		nServices = 1
	}
	services := make([]string, nServices)
	for i := range services {
		services[i] = fmt.Sprintf("svc-%02d", i)
	}
	parsedLevels := splitCSV(levels)
	if len(parsedLevels) == 0 {
		parsedLevels = []string{"info"}
	}

	if msgBytes < 32 {
		msgBytes = 32
	}
	filler := make([]byte, msgBytes)
	const alphabet = "abcdefghijklmnopqrstuvwxyz "
	for i := range filler {
		filler[i] = alphabet[r.IntN(len(alphabet))]
	}

	hn, _ := os.Hostname()
	if hn == "" {
		hn = "loadgen"
	}

	return &eventGenerator{
		rng:       r,
		services:  services,
		levels:    parsedLevels,
		msgFiller: filler,
		hostname:  hn,
	}
}

// batch returns an NDJSON body containing n synthetic events.
func (g *eventGenerator) batch(n int) []byte {
	var buf bytes.Buffer
	buf.Grow(n * (len(g.msgFiller) + 200))

	g.mu.Lock()
	now := time.Now().UTC()
	for i := 0; i < n; i++ {
		g.counter++
		svc := g.services[g.rng.IntN(len(g.services))]
		lvl := g.levels[g.rng.IntN(len(g.levels))]
		userID := g.rng.Uint32N(10_000)
		duration := g.rng.Uint32N(5_000)
		seq := g.counter

		_, _ = buf.WriteString(`{"timestamp":"`)
		_, _ = buf.WriteString(now.Add(time.Duration(i) * time.Microsecond).Format(time.RFC3339Nano))
		_, _ = buf.WriteString(`","service":"`)
		_, _ = buf.WriteString(svc)
		_, _ = buf.WriteString(`","level":"`)
		_, _ = buf.WriteString(lvl)
		_, _ = buf.WriteString(`","host":"`)
		_, _ = buf.WriteString(g.hostname)
		_, _ = buf.WriteString(`","message":"loadgen seq=`)
		_, _ = fmt.Fprintf(&buf, "%d", seq)
		_, _ = buf.WriteString(` filler=`)
		_, _ = buf.Write(g.msgFiller)
		_, _ = buf.WriteString(`","user_id":`)
		_, _ = fmt.Fprintf(&buf, "%d", userID)
		_, _ = buf.WriteString(`,"duration_ms":`)
		_, _ = fmt.Fprintf(&buf, "%d", duration)
		_, _ = buf.WriteString("}\n")
	}
	g.mu.Unlock()

	return buf.Bytes()
}

func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	out := make([]string, 0, 4)
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}
