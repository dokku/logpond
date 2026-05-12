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

	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/dokku/logpond/internal/api"
	"github.com/dokku/logpond/internal/archive"
	"github.com/dokku/logpond/internal/catalog"
	"github.com/dokku/logpond/internal/config"
	"github.com/dokku/logpond/internal/facets"
	"github.com/dokku/logpond/internal/ingest"
	"github.com/dokku/logpond/internal/jobs"
	"github.com/dokku/logpond/internal/metrics"
	"github.com/dokku/logpond/internal/query"
	"github.com/dokku/logpond/internal/retention"
	"github.com/dokku/logpond/internal/segments"
	"github.com/dokku/logpond/internal/ui"
	"github.com/dokku/logpond/internal/ws"
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

	fanout := ingest.NewFanout()
	flusher := ingest.NewFlusher(buf, time.Second, bufCap, func(ctx context.Context, batch []ingest.Event) {
		if err := mgr.Flush(ctx, batch); err != nil {
			logger.Error("flushing batch to segment", "count", len(batch), "err", err)
		}
		fanout.Publish(batch)
	}, logger)
	liveTail := ws.New(ws.Options{Fanout: fanout, Logger: logger})

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

	archiveBackend, err := buildArchiveBackend(ctx, cfg, logger)
	if err != nil {
		return fmt.Errorf("configuring archive backend: %w", err)
	}

	retentionEval, retentionInterval, err := buildRetention(cfg, cat, archiveBackend, logger)
	if err != nil {
		return fmt.Errorf("configuring retention: %w", err)
	}

	jobManager, err := jobs.New(jobs.Options{Catalog: cat, Logger: logger})
	if err != nil {
		return fmt.Errorf("configuring jobs manager: %w", err)
	}

	rehydrationTTL := time.Duration(cfg.Rehydration.TTLDays) * 24 * time.Hour
	if rehydrationTTL <= 0 {
		rehydrationTTL = 7 * 24 * time.Hour
	}

	srv := api.New(api.Options{
		Logger:          logger,
		Buffer:          buf,
		Metrics:         m,
		Extractors:      extractors,
		Executor:        executor,
		Facets:          facetRegistry,
		Retention:       retentionEval,
		Catalog:         cat,
		ArchiveBackend:  archiveBackend,
		Jobs:            jobManager,
		MaxTimeRange:    maxTimeRange,
		FacetSampleSize: cfg.Facets.SegmentSampleSize,
		DataDir:         cfg.DataDir,
		RehydrationTTL:  rehydrationTTL,
		Fanout:          fanout,
		LiveTail:        http.HandlerFunc(liveTail.Handle),
	})

	uiSrv, err := ui.New(ui.Options{
		Logger:          logger,
		Executor:        executor,
		Facets:          facetRegistry,
		Jobs:            jobManager,
		MaxTimeRange:    int64(maxTimeRange / time.Second),
		FacetSampleSize: cfg.Facets.SegmentSampleSize,
		Archive:         describeArchive(cfg, archiveBackend),
		Retention: ui.RetentionInfo{
			MaxAge:              cfg.Retention.MaxAge,
			MaxSize:             cfg.Retention.MaxSize,
			ArchiveBeforeDelete: cfg.Retention.ArchiveBeforeDelete,
		},
	})
	if err != nil {
		return fmt.Errorf("configuring ui: %w", err)
	}
	uiSrv.Register(srv.Router())

	// A second WebSocket server emits HTML oob-swap fragments for the
	// /ui/query/stream live tail view. /api/query/stream stays JSON for
	// the programmatic API contract (PRD §13.7).
	htmlTail := ws.New(ws.Options{
		Fanout:   fanout,
		Logger:   logger,
		Renderer: uiSrv.NewHTMLStreamRenderer(),
	})
	srv.Router().Get("/ui/query/stream", htmlTail.Handle)

	importWatcher := api.NewImportWatcher(api.ImportWatcherOptions{
		DataDir: cfg.DataDir,
		Catalog: cat,
		TTL:     rehydrationTTL,
		Logger:  logger,
	})
	evictor := api.NewEvictor(api.EvictorOptions{
		Catalog:  cat,
		Executor: executor,
		Logger:   logger,
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
	go func() {
		defer wg.Done()
		jobManager.RunGCLoop(ctx, 0)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		importWatcher.Run(ctx, 30*time.Second)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		evictor.Run(ctx, 5*time.Minute)
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
func buildRetention(cfg *config.Config, cat *catalog.Catalog, backend archive.Backend, logger *slog.Logger) (*retention.Evaluator, time.Duration, error) {
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
	backendActive := backend != nil && backend.Capabilities().Name != "none"
	eval, err := retention.New(retention.Options{
		Catalog:             cat,
		MaxAge:              maxAge,
		MaxSize:             maxSize,
		ArchiveBeforeDelete: cfg.Retention.ArchiveBeforeDelete,
		BackendActive:       backendActive,
		Backend:             backend,
		Logger:              logger,
	})
	if err != nil {
		return nil, 0, err
	}
	return eval, interval, nil
}

// buildArchiveBackend instantiates the configured archive backend.
// archive.backend=none returns a NoneBackend; s3 builds a real client
// using the AWS SDK. The script backend is wired in Phase 9.
func buildArchiveBackend(ctx context.Context, cfg *config.Config, logger *slog.Logger) (archive.Backend, error) {
	switch cfg.Archive.Backend {
	case "", "none":
		return archive.NoneBackend{}, nil
	case "s3":
		loadOpts := []func(*awscfg.LoadOptions) error{}
		if cfg.Archive.S3.Region != "" {
			loadOpts = append(loadOpts, awscfg.WithRegion(cfg.Archive.S3.Region))
		}
		if cfg.Archive.S3.AccessKeyID != "" && cfg.Archive.S3.SecretAccessKey != "" {
			loadOpts = append(loadOpts, awscfg.WithCredentialsProvider(
				credentials.NewStaticCredentialsProvider(cfg.Archive.S3.AccessKeyID, cfg.Archive.S3.SecretAccessKey, "")))
		}
		awsCfg, err := awscfg.LoadDefaultConfig(ctx, loadOpts...)
		if err != nil {
			return nil, fmt.Errorf("loading aws config: %w", err)
		}
		client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
			if cfg.Archive.S3.Endpoint != "" {
				o.BaseEndpoint = &cfg.Archive.S3.Endpoint
				o.UsePathStyle = true
			}
		})
		return archive.NewS3Backend(archive.S3Options{
			Client: client,
			Bucket: cfg.Archive.S3.Bucket,
			Prefix: cfg.Archive.S3.Prefix,
			Logger: logger,
		})
	case "script":
		if cfg.Archive.Script.Path == "" {
			return nil, fmt.Errorf("archive.script.path is required when archive.backend=script")
		}
		timeout := archive.DefaultScriptTimeout
		if cfg.Archive.Script.Timeout != "" {
			d, err := config.ParseDuration(cfg.Archive.Script.Timeout)
			if err != nil {
				return nil, fmt.Errorf("parsing archive.script.timeout: %w", err)
			}
			if d > 0 {
				timeout = d
			}
		}
		workDir := filepath.Join(cfg.DataDir, "script-work")
		be, err := archive.NewScriptBackend(archive.ScriptOptions{
			Path:    cfg.Archive.Script.Path,
			Timeout: timeout,
			Env:     cfg.Archive.Script.Env,
			WorkDir: workDir,
			Logger:  logger,
		})
		if err != nil {
			return nil, err
		}
		probeCtx, cancel := context.WithTimeout(ctx, timeout+30*time.Second)
		defer cancel()
		if err := be.Probe(probeCtx); err != nil {
			logger.Warn("archive: script probe failed; continuing with defaults", "err", err)
		}
		return be, nil
	default:
		return nil, fmt.Errorf("unsupported archive backend %q", cfg.Archive.Backend)
	}
}

// describeArchive renders a short, human-readable summary of the active
// archive backend for display in the admin UI and top bar.
func describeArchive(cfg *config.Config, backend archive.Backend) ui.ArchiveInfo {
	kind := cfg.Archive.Backend
	if kind == "" {
		kind = "none"
	}
	info := ui.ArchiveInfo{Kind: kind}
	switch kind {
	case "s3":
		info.Detail = fmt.Sprintf("s3://%s/%s", cfg.Archive.S3.Bucket, cfg.Archive.S3.Prefix)
	case "script":
		info.Detail = cfg.Archive.Script.Path
	case "none":
		info.Detail = "no archive backend configured"
	}
	if backend != nil {
		_ = backend.Capabilities()
	}
	return info
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
