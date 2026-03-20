package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/stevenvi/bokeh-mediaserver/internal/api"
	"github.com/stevenvi/bokeh-mediaserver/internal/config"
	"github.com/stevenvi/bokeh-mediaserver/internal/db"
	"github.com/stevenvi/bokeh-mediaserver/internal/imaging"
	"github.com/stevenvi/bokeh-mediaserver/internal/indexer"
	"github.com/stevenvi/bokeh-mediaserver/internal/jobs"
	"github.com/stevenvi/bokeh-mediaserver/internal/maintenance"
	"github.com/stevenvi/bokeh-mediaserver/internal/repository"
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
	db_pool, err := db.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("db: %w", err)
	}
	defer db_pool.Close()

	// ── Imaging module initialization ─────────────────────────────────────────
	slog.Info("initializing imaging module")
	imaging.Startup()
	defer imaging.Shutdown()

	// ── Startup recovery ──────────────────────────────────────────────────────
	slog.Info("running startup recovery")
	jobRepo := repository.NewJobRepository(db_pool)
	mediaRepo := repository.NewMediaItemRepository(db_pool)
	if err := jobRepo.RecoverStuck(ctx); err != nil {
		return fmt.Errorf("recovery: %w", err)
	}
	if count, err := mediaRepo.CountPendingVariants(ctx); err != nil {
		return fmt.Errorf("count incomplete variants: %w", err)
	} else if count > 0 {
		slog.Warn("photos pending variant generation — will process on next scan",
			"count", count)
	}

	// ── Ensure data path exists ───────────────────────────────────────────────
	if err := os.MkdirAll(cfg.DataPath, 0o755); err != nil {
		return fmt.Errorf("data path: %w", err)
	}

	// ── Device guard (load banned devices) ────────────────────────────────────
	guard := api.NewDeviceGuard()
	if err := guard.LoadBanned(ctx, db_pool); err != nil {
		return fmt.Errorf("load banned devices: %w", err)
	}

	// ── Worker pools ──────────────────────────────────────────────────────────
	mainPool := jobs.NewPool(cfg.WorkerCount)
	processingPool := jobs.NewPool(cfg.ProcessingWorkerCount)

	// Create per-worker exiftool process managers for the processing pool
	processingWorkers := indexer.NewProcessingWorkers(cfg.ProcessingWorkerCount)
	defer processingWorkers.CloseAll()

	// ── Job dispatcher ────────────────────────────────────────────────────────
	dispatcher := jobs.NewDispatcher(db_pool, mainPool, processingPool)
	dispatcher.Register("library_scan", indexer.HandleScanJob(cfg.MediaPath, cfg.DataPath), false)
	dispatcher.Register("process_media", indexer.HandleProcessMediaWithWorkers(processingWorkers, cfg.MediaPath, cfg.DataPath), true)
	dispatcher.Register("orphan_cleanup", maintenance.HandleOrphanCleanup(cfg.DataPath), false)
	dispatcher.Register("integrity_check", maintenance.HandleIntegrityCheck(cfg.DataPath), false)
	dispatcher.Register("device_cleanup", maintenance.HandleDeviceCleanup(), false)
	dispatcher.Start(ctx)

	// ── Scheduler ─────────────────────────────────────────────────────────────
	scheduler := jobs.NewScheduler(db_pool)
	scheduler.Start(ctx)

	// ── HTTP server ───────────────────────────────────────────────────────────
	router := api.NewRouter(db_pool, mainPool, guard, cfg.JWTSecret, cfg.MediaPath, cfg.DataPath)

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

	// Stop scheduler first — no more new jobs
	scheduler.Stop()

	shutdownCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Stop accepting new HTTP requests
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("server shutdown: %w", err)
	}

	// Stop dispatcher — waits for in-flight jobs
	slog.Info("waiting for job dispatcher to finish")
	dispatcher.Stop()

	// Close worker pools
	mainPool.Close()
	processingPool.Close()

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

	var out *os.File = os.Stdout
	if logPath != "" {
		f, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			// Can't use slog here yet — it isn't set up. Write directly.
			fmt.Fprintf(os.Stderr, "could not open log file %s: %v, falling back to stdout\n", logPath, err)
		} else {
			out = f
		}
	}

	slog.SetDefault(slog.New(&flatHandler{out: out, level: lvl}))
}

// flatHandler formats log lines as:
// [2006-01-02 15:04:05] [LEVEL] [internal/indexer/indexer.go:42] message key=value key=value
type flatHandler struct {
	out   *os.File
	level slog.Level
	mu    sync.Mutex
}

func (h *flatHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.level
}

func (h *flatHandler) Handle(_ context.Context, r slog.Record) error {
	// [timestamp]
	ts := r.Time.Format("2006-01-02T15:04:05")

	// [level] — fixed width for alignment
	lvl := fmt.Sprintf("%-5s", r.Level.String())

	// [source] — requires AddSource in the record, populated by runtime.Callers.
	// slog populates this when slog.HandlerOptions.AddSource is true, but since
	// we're implementing the handler directly we read it from the record's PC.
	src := ""
	if r.PC != 0 {
		frames := runtime.CallersFrames([]uintptr{r.PC})
		frame, _ := frames.Next()
		// Trim to module-relative path: everything from "internal/" onwards
		file := frame.File
		if i := strings.Index(file, "internal/"); i >= 0 {
			file = file[i:]
		} else {
			file = filepath.Base(file)
		}
		src = fmt.Sprintf("%s:%d", file, frame.Line)
	}

	// key=value attrs
	var attrs []string
	r.Attrs(func(a slog.Attr) bool {
		attrs = append(attrs, fmt.Sprintf("%s=%v", a.Key, a.Value))
		return true
	})

	line := fmt.Sprintf("%s [%s] [%s] %s", ts, lvl, src, r.Message)
	if len(attrs) > 0 {
		line += " " + strings.Join(attrs, " ")
	}
	line += "\n"

	h.mu.Lock()
	defer h.mu.Unlock()
	_, err := fmt.Fprint(h.out, line)
	return err
}

func (h *flatHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return h
}

func (h *flatHandler) WithGroup(name string) slog.Handler {
	return h
}
