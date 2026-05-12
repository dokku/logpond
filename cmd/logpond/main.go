package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/dokku/logpond/internal/catalog"
	"github.com/dokku/logpond/internal/config"
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

	<-ctx.Done()
	if err := context.Cause(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Info("shutting down", "cause", err)
	} else {
		logger.Info("shutting down")
	}
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
