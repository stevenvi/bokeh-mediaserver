package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/stevenvi/bokeh-mediaserver/internal/api"
	"github.com/stevenvi/bokeh-mediaserver/internal/config"
	"github.com/stevenvi/bokeh-mediaserver/internal/db"
	"github.com/stevenvi/bokeh-mediaserver/internal/imaging"
	"github.com/stevenvi/bokeh-mediaserver/internal/jobs"
)

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run() error {
	// ── Config ────────────────────────────────────────────────────────────────
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	setupLogger(cfg.LogLevel, cfg.LogPath)

	// ── Migrations ────────────────────────────────────────────────────────────
	slog.Info("running database migrations")
	if err := db.Migrate(cfg.DatabaseURL); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	slog.Info("migrations complete")

	// ── Database pool ─────────────────────────────────────────────────────────
	ctx := context.Background()
	pool, err := db.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("db: %w", err)
	}
	defer pool.Close()

	// ── Imaging module initialization ─────────────────────────────────────────
	slog.Info("initializing imaging module")
	imaging.Startup()
	defer imaging.Shutdown()

	// ── Startup recovery ──────────────────────────────────────────────────────
	slog.Info("running startup recovery")
	if err := jobs.RecoverStuckJobs(ctx, pool); err != nil {
		return fmt.Errorf("recovery: %w", err)
	}

	// ── Ensure data path exists ───────────────────────────────────────────────
	if err := os.MkdirAll(cfg.DataPath, 0o755); err != nil {
		return fmt.Errorf("data path: %w", err)
	}

	// ── Worker pool ───────────────────────────────────────────────────────────
	workerPool := jobs.NewPool(cfg.WorkerCount)

	// ── HTTP server ───────────────────────────────────────────────────────────
	router := api.NewRouter(pool, workerPool, cfg.JWTSecret, cfg.MediaPath, cfg.DataPath)

	srv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      router,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 120 * time.Second, // longer for SSE and large image responses
		IdleTimeout:  120 * time.Second,
	}

	// ── Graceful shutdown ─────────────────────────────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		slog.Info("server starting", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server error", "err", err)
			os.Exit(1)
		}
	}()

	<-quit
	slog.Info("shutdown signal received")

	shutdownCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Stop accepting new requests; let in-flight requests finish
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("server shutdown: %w", err)
	}

	// Wait for any running worker pool jobs to complete
	slog.Info("waiting for background workers to finish")
	workerPool.Wait()

	slog.Info("shutdown complete")
	return nil
}

func setupLogger(level, logPath string) {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "info":
		lvl = slog.LevelInfo
	case "warn":
		lvl = slog.LevelWarn
	default:
		lvl = slog.LevelError
	}

	opts := &slog.HandlerOptions{Level: lvl}

	var out *os.File = os.Stdout
	if logPath != "" {
		f, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			slog.Warn("could not open log file, falling back to stdout", "path", logPath, "err", err)
		} else {
			out = f
		}
	}

	slog.SetDefault(slog.New(slog.NewJSONHandler(out, opts)))
}
