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
	"github.com/dokku/logpond/internal/ingest"
	"github.com/dokku/logpond/internal/metrics"
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

	flusher := ingest.NewFlusher(buf, time.Second, bufCap, nil, logger)

	srv := api.New(api.Options{
		Logger:     logger,
		Buffer:     buf,
		Metrics:    m,
		Extractors: extractors,
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
