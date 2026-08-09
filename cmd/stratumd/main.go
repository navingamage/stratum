// Command stratumd runs the stratum server: a time-series database with an
// HTTP ingest and query interface.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/navingamage/stratum/internal/api"
	"github.com/navingamage/stratum/internal/buildinfo"
	"github.com/navingamage/stratum/internal/query"
	"github.com/navingamage/stratum/internal/tsdb"
	"github.com/navingamage/stratum/internal/wal"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "stratumd: %v\n", err)
		os.Exit(1)
	}
}

type config struct {
	dataDir string
	listen  string

	blockDuration time.Duration
	retention     time.Duration
	walSync       string

	maxSamples    int
	queryTimeout  time.Duration
	lookbackDelta time.Duration

	logLevel    string
	showVersion bool
}

func run() error {
	var cfg config

	fs := flag.NewFlagSet("stratumd", flag.ContinueOnError)
	fs.StringVar(&cfg.dataDir, "data-dir", "./data", "directory to store blocks and the write-ahead log in")
	fs.StringVar(&cfg.listen, "listen", ":9090", "address to serve HTTP on")
	fs.DurationVar(&cfg.blockDuration, "block-duration", 2*time.Hour, "time span the head accumulates before being flushed to a block")
	fs.DurationVar(&cfg.retention, "retention", 0, "delete blocks older than this; zero keeps everything")
	fs.StringVar(&cfg.walSync, "wal-sync", "interval", "write-ahead log durability: always, interval or never")
	fs.IntVar(&cfg.maxSamples, "query-max-samples", query.DefaultMaxSamples, "maximum samples a single query may load")
	fs.DurationVar(&cfg.queryTimeout, "query-timeout", query.DefaultTimeout, "maximum time a single query may run")
	fs.DurationVar(&cfg.lookbackDelta, "query-lookback-delta", 5*time.Minute, "how far back an instant selector looks for a sample")
	fs.StringVar(&cfg.logLevel, "log-level", "info", "log level: debug, info, warn or error")
	fs.BoolVar(&cfg.showVersion, "version", false, "print the version and exit")

	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "%s\n\nUsage of stratumd:\n", buildinfo.String())
		fs.PrintDefaults()
	}

	if err := fs.Parse(os.Args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if cfg.showVersion {
		fmt.Println(buildinfo.String())
		return nil
	}

	level, err := parseLogLevel(cfg.logLevel)
	if err != nil {
		return err
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	syncPolicy, err := parseSyncPolicy(cfg.walSync)
	if err != nil {
		return err
	}

	logger.Info("starting", "version", buildinfo.String(),
		"dataDir", cfg.dataDir, "listen", cfg.listen)

	db, err := tsdb.Open(cfg.dataDir, tsdb.Options{
		BlockDuration: cfg.blockDuration.Milliseconds(),
		Retention:     cfg.retention,
		WALSync:       syncPolicy,
		Logger:        logger,
	})
	if err != nil {
		return fmt.Errorf("opening the database: %w", err)
	}

	engine := query.NewEngine(query.EngineOptions{
		MaxSamples:    cfg.maxSamples,
		Timeout:       cfg.queryTimeout,
		LookbackDelta: cfg.lookbackDelta.Milliseconds(),
	})

	srv := &http.Server{
		Addr:    cfg.listen,
		Handler: api.New(db, engine, api.Options{Logger: logger}).Handler(),

		// A query can legitimately run for the query timeout, so the write
		// deadline has to exceed it or long queries would be cut off by the
		// server rather than by their own budget.
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       time.Minute,
		WriteTimeout:      cfg.queryTimeout + 30*time.Second,
		IdleTimeout:       2 * time.Minute,
	}

	errc := make(chan error, 1)
	go func() {
		logger.Info("listening", "addr", cfg.listen)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errc <- err
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	select {
	case err := <-errc:
		db.Close()
		return fmt.Errorf("serving: %w", err)
	case <-ctx.Done():
		logger.Info("shutting down")
	}

	// Stop accepting requests before closing the database, so an in-flight
	// query cannot outlive the storage it is reading.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var shutdownErr error
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("the HTTP server did not shut down cleanly", "err", err)
		shutdownErr = err
	}

	if err := db.Close(); err != nil {
		logger.Error("closing the database", "err", err)
		if shutdownErr == nil {
			shutdownErr = err
		}
	}

	logger.Info("stopped")
	return shutdownErr
}

func parseLogLevel(s string) (slog.Level, error) {
	switch s {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	}
	return 0, fmt.Errorf("unknown log level %q; want debug, info, warn or error", s)
}

func parseSyncPolicy(s string) (wal.SyncPolicy, error) {
	switch s {
	case "always":
		return wal.SyncAlways, nil
	case "interval":
		return wal.SyncInterval, nil
	case "never":
		return wal.SyncNever, nil
	}
	return 0, fmt.Errorf("unknown wal-sync policy %q; want always, interval or never", s)
}
