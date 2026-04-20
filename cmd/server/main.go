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
	"github.com/stevenvi/bokeh-mediaserver/internal/jobs"
	job_definitions "github.com/stevenvi/bokeh-mediaserver/internal/jobs/definitions"
	"github.com/stevenvi/bokeh-mediaserver/internal/repository"
	"github.com/stevenvi/bokeh-mediaserver/internal/streaming"
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

	slog.Info("bokeh media server starting", "cpus", runtime.NumCPU())

	// ── Migrations ────────────────────────────────────────────────────────────
	slog.Info("running database migrations")
	if err := db.Migrate(cfg.DatabaseURL); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	slog.Info("migrations complete")

	// ── Database pool ─────────────────────────────────────────────────────────
	ctx := context.Background()
	db, err := db.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("db: %w", err)
	}
	defer db.Close()

	// ── Imaging module initialization ─────────────────────────────────────────
	slog.Info("initializing imaging module")
	imaging.Startup()
	defer imaging.Shutdown()

	// ── Startup recovery ──────────────────────────────────────────────────────
	slog.Info("running startup recovery")

	// Load transcode bitrate from server_config
	if kbps, err := repository.ServerConfigTranscodeBitrate(ctx, db); err != nil {
		slog.Warn("could not load transcode_bitrate_kbps; using default", "err", err)
	} else {
		cfg.TranscodeBitrateKbps = kbps
	}

	if err := repository.JobsResetStuck(ctx, db); err != nil {
		return fmt.Errorf("recovery: %w", err)
	}
	if count, err := repository.PhotoCountPendingVariants(ctx, db); err != nil {
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
	if err := guard.LoadBanned(ctx, db); err != nil {
		return fmt.Errorf("load banned devices: %w", err)
	}

	// ── Job dispatcher ────────────────────────────────────────────────────────
	dispatcher := jobs.NewDispatcher(db)
	dispatcher.Start(ctx)
	dispatcher.Register("collection_scan", job_definitions.CollectionScanMeta, job_definitions.HandleCollectionScan(cfg.MediaPath, cfg.DataPath))
	dispatcher.Register("scan_photo", job_definitions.ScanPhotoMeta, job_definitions.HandleScanPhoto(cfg.MediaPath, cfg.DataPath))
	dispatcher.Register("scan_video", job_definitions.ScanVideoMeta, job_definitions.HandleScanVideo(cfg.MediaPath, cfg.DataPath, cfg.TranscodeBitrateKbps))
	dispatcher.Register("scan_audio", job_definitions.ScanAudioMeta, job_definitions.HandleScanAudio(cfg.MediaPath, cfg.DataPath))
	dispatcher.Register("thumbnail_scan", job_definitions.ThumbnailScanMeta, job_definitions.HandleThumbnailScan(cfg.MediaPath, cfg.DataPath))
	dispatcher.Register("video_transcode", job_definitions.VideoTranscodeMeta, job_definitions.HandleVideoTranscode())
	dispatcher.Register("video_transcode_item", job_definitions.VideoTranscodeItemMeta, job_definitions.HandleVideoTranscodeItem(cfg))
	dispatcher.Register("orphan_cleanup", job_definitions.OrphanMeta, job_definitions.HandleOrphanCleanup(cfg.DataPath))
	dispatcher.Register("integrity_check", job_definitions.IntegrityCheckMeta, job_definitions.HandleIntegrityCheck(cfg.DataPath, dispatcher))
	dispatcher.Register("device_cleanup", job_definitions.DeviceCleanupMeta, job_definitions.HandleDeviceCleanup())
	dispatcher.Register("cover_cycle", job_definitions.CoverCycleMeta, job_definitions.HandleCoverCycle(cfg.DataPath))
	dispatcher.Register("dzi_gen", job_definitions.DZIGenMeta, job_definitions.HandleDZIGen(cfg.MediaPath, cfg.DataPath))
	dispatcher.Register("dzi_gen_item", job_definitions.DZIGenItemMeta, job_definitions.HandleDZIGenItem(cfg.MediaPath, cfg.DataPath))

	// ── Streaming idle sweeper ─────────────────────────────────────────────────
	streaming.StartIdleSweeper(ctx)

	// ── Scheduler ─────────────────────────────────────────────────────────────
	scheduler := jobs.NewScheduler(db, dispatcher)
	scheduler.Start(ctx)

	// ── HTTP server ───────────────────────────────────────────────────────────
	router := api.NewRouter(db, guard, dispatcher, scheduler, cfg, cfg.JWTSecret, cfg.MediaPath, cfg.DataPath, cfg.ClientOrigin, cfg.Production)

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
