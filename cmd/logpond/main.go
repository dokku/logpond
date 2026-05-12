package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/dokku/logpond/internal/api"
	"github.com/dokku/logpond/internal/catalog"
	"github.com/dokku/logpond/internal/config"
	"github.com/dokku/logpond/internal/facets"
	"github.com/dokku/logpond/internal/ingest"
	"github.com/dokku/logpond/internal/metrics"
	"github.com/dokku/logpond/internal/query"
	"github.com/dokku/logpond/internal/retention"
	"github.com/dokku/logpond/internal/segments"
)

var version = "0.0.0-dev"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "logpond:", err)
		os.Exit(1)
	}
}

func run() error {
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()
	if *showVersion {
		fmt.Println("logpond", version)
		return nil
	}

	configPath := os.Getenv("LOGPOND_CONFIG")
	if configPath == "" {
		configPath = "/etc/logpond/config.yaml"
	}
	cfg, err := config.Load(configPath)
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	logger := newLogger(cfg.LogLevel)
	slog.SetDefault(logger)

	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return fmt.Errorf("ensuring data_dir %s: %w", cfg.DataDir, err)
	}

	redactedJSON, _ := json.Marshal(cfg.Redacted())
	logger.Info("logpond starting",
		"version", version,
		"config_path", configPath,
		"data_dir", cfg.DataDir,
		"listen", fmt.Sprintf("%s:%d", cfg.ListenAddress, cfg.Port),
		"config", json.RawMessage(redactedJSON),
	)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	catalogPath := filepath.Join(cfg.DataDir, "catalog.db")
	cat, err := catalog.Open(ctx, catalogPath, nil)
	if err != nil {
		return fmt.Errorf("opening catalog: %w", err)
	}
	defer func() {
		if err := cat.Close(); err != nil {
			logger.Warn("closing catalog", "err", err)
		}
	}()

	applied, err := cat.AppliedMigrations(ctx)
	if err != nil {
		return fmt.Errorf("reading migrations: %w", err)
	}
	logger.Info("catalog ready", "path", catalogPath, "migrations", applied)

	bufCap, err := ringBufferEventCap(cfg.MemoryLimits.RingBuffer)
	if err != nil {
		return fmt.Errorf("computing ring buffer capacity: %w", err)
	}
	buf := ingest.NewBuffer(bufCap)
	logger.Info("ring buffer ready", "capacity_events", bufCap, "memory_limit", cfg.MemoryLimits.RingBuffer)

	m := metrics.New(metrics.Options{FillRatio: buf.FillRatio})

	extractors := make(map[string]*ingest.Extractor, len(cfg.Sources))
	for _, src := range cfg.Sources {
		extractors[src.Name] = ingest.NewExtractor(ingest.SourceFromConfig(src.Name, src.Extract))
	}

	window, err := config.ParseDuration(cfg.SegmentWindow)
	if err != nil {
		return fmt.Errorf("parsing segment_window: %w", err)
	}
	sealInterval, err := config.ParseDuration(cfg.SealingInterval)
	if err != nil {
		return fmt.Errorf("parsing sealing_interval: %w", err)
	}
	mgr, err := segments.New(segments.Options{
		DataDir:     cfg.DataDir,
		Window:      window,
		SealGrace:   sealInterval,
		MemoryLimit: cfg.MemoryLimits.DuckDB,
		Catalog:     cat,
		Logger:      logger,
	})
	if err != nil {
		return fmt.Errorf("starting segment manager: %w", err)
	}
	defer func() {
		if err := mgr.Close(); err != nil {
			logger.Warn("closing segment manager", "err", err)
		}
	}()

	rec, err := mgr.Recover(ctx)
	if err != nil {
		return fmt.Errorf("recovering segments: %w", err)
	}
	logger.Info("segment recovery complete",
		"active_registered", len(rec.ActiveRegistered),
		"sealed_registered", len(rec.SealedRegistered),
		"orphans_moved", len(rec.OrphansMoved),
		"lost_marked", len(rec.LostMarked),
	)

	flusher := ingest.NewFlusher(buf, time.Second, bufCap, func(ctx context.Context, batch []ingest.Event) {
		if err := mgr.Flush(ctx, batch); err != nil {
			logger.Error("flushing batch to segment", "count", len(batch), "err", err)
		}
	}, logger)

	maxTimeRange, err := config.ParseDuration(cfg.Query.MaxTimeRange)
	if err != nil {
		return fmt.Errorf("parsing query.max_time_range: %w", err)
	}
	executor := query.NewExecutor(cat, mgr, logger)

	facetRegistry := facets.New(facets.Options{Catalog: cat, Logger: logger})
	if err := facetRegistry.Load(ctx, cfg); err != nil {
		return fmt.Errorf("loading facets: %w", err)
	}
	for _, warn := range facetRegistry.Warnings() {
		logger.Warn("facet registry warning", "msg", warn)
	}

	retentionEval, retentionInterval, err := buildRetention(cfg, cat, logger)
	if err != nil {
		return fmt.Errorf("configuring retention: %w", err)
	}

	srv := api.New(api.Options{
		Logger:          logger,
		Buffer:          buf,
		Metrics:         m,
		Extractors:      extractors,
		Executor:        executor,
		Facets:          facetRegistry,
		Retention:       retentionEval,
		MaxTimeRange:    maxTimeRange,
		FacetSampleSize: cfg.Facets.SegmentSampleSize,
	})

	httpServer := &http.Server{
		Addr:              fmt.Sprintf("%s:%d", cfg.ListenAddress, cfg.Port),
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		flusher.Run(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		mgr.RunSealLoop(ctx, sealInterval)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		retentionEval.RunLoop(ctx, retentionInterval)
	}()

	wg.Add(1)
	httpErr := make(chan error, 1)
	go func() {
		defer wg.Done()
		ln, err := net.Listen("tcp", httpServer.Addr)
		if err != nil {
			httpErr <- err
			return
		}
		logger.Info("http server listening", "addr", ln.Addr().String())
		if err := httpServer.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			httpErr <- err
		}
	}()

	select {
	case <-ctx.Done():
	case err := <-httpErr:
		cancel()
		wg.Wait()
		return fmt.Errorf("http server: %w", err)
	}

	if err := context.Cause(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Info("shutting down", "cause", err)
	} else {
		logger.Info("shutting down")
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Warn("http shutdown", "err", err)
	}
	wg.Wait()
	return nil
}

func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		lvl = slog.LevelInfo
	}
	h := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: lvl})
	return slog.New(h)
}

// buildRetention parses the retention block in cfg and returns a
// ready-to-run evaluator alongside the desired loop interval. An empty
// policy still yields a valid Evaluator; RunLoop blocks until ctx ends
// without doing any work in that case.
func buildRetention(cfg *config.Config, cat *catalog.Catalog, logger *slog.Logger) (*retention.Evaluator, time.Duration, error) {
	var maxAge time.Duration
	if cfg.Retention.MaxAge != "" {
		d, err := config.ParseDuration(cfg.Retention.MaxAge)
		if err != nil {
			return nil, 0, fmt.Errorf("parsing retention.max_age: %w", err)
		}
		maxAge = d
	}
	var maxSize int64
	if cfg.Retention.MaxSize != "" {
		n, err := config.ParseSize(cfg.Retention.MaxSize)
		if err != nil {
			return nil, 0, fmt.Errorf("parsing retention.max_size: %w", err)
		}
		maxSize = n
	}
	interval := retention.DefaultEvaluationInterval
	if cfg.Retention.EvaluationInterval != "" {
		d, err := config.ParseDuration(cfg.Retention.EvaluationInterval)
		if err != nil {
			return nil, 0, fmt.Errorf("parsing retention.evaluation_interval: %w", err)
		}
		if d > 0 {
			interval = d
		}
	}
	eval, err := retention.New(retention.Options{
		Catalog:             cat,
		MaxAge:              maxAge,
		MaxSize:             maxSize,
		ArchiveBeforeDelete: cfg.Retention.ArchiveBeforeDelete,
		BackendActive:       cfg.Archive.Backend != "" && cfg.Archive.Backend != "none",
		Logger:              logger,
	})
	if err != nil {
		return nil, 0, err
	}
	return eval, interval, nil
}

// ringBufferEventCap converts the configured ring-buffer memory limit
// to an event count using a 1KB-per-event heuristic. See
// docs/IMPLEMENTATION-NOTES.md for the rationale.
func ringBufferEventCap(memoryLimit string) (int, error) {
	bytes, err := config.ParseSize(memoryLimit)
	if err != nil {
		return 0, fmt.Errorf("parsing %q: %w", memoryLimit, err)
	}
	if bytes <= 0 {
		bytes = 50 << 20 // 50MB default
	}
	const bytesPerEvent = 1024
	n := bytes / bytesPerEvent
	if n < 1 {
		n = 1
	}
	// Guard against absurdly large configurations that would happily
	// allocate a multi-million-element slice up front.
	const hardCap = 1_000_000
	if n > hardCap {
		n = hardCap
	}
	return int(n), nil
}
