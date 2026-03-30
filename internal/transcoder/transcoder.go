package transcoder

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/stevenvi/bokeh-mediaserver/internal/config"
	"github.com/stevenvi/bokeh-mediaserver/internal/imaging"
	"github.com/stevenvi/bokeh-mediaserver/internal/models"
	"github.com/stevenvi/bokeh-mediaserver/internal/repository"
	"github.com/stevenvi/bokeh-mediaserver/internal/utils"
)

var (
	activeMu   sync.Mutex
	activeCmds = map[int64]*exec.Cmd{} // job ID → running ffmpeg process
	isPaused   bool
)

// PauseActive sends SIGSTOP to all currently running background transcodes.
// No-op if no transcodes are running or they are already paused.
//
// TODO: SIGSTOP is not supported on Windows. If Windows support is needed,
// consider using Windows Job Objects or a similar suspension mechanism.
func PauseActive() {
	activeMu.Lock()
	defer activeMu.Unlock()
	if isPaused {
		return
	}
	for _, cmd := range activeCmds {
		if cmd.Process != nil {
			_ = cmd.Process.Signal(syscall.SIGSTOP)
		}
	}
	isPaused = true
}

// ResumeActive sends SIGCONT to all previously paused background transcodes.
// No-op if no transcodes are running or they are not paused.
func ResumeActive() {
	activeMu.Lock()
	defer activeMu.Unlock()
	if !isPaused {
		return
	}
	for _, cmd := range activeCmds {
		if cmd.Process != nil {
			_ = cmd.Process.Signal(syscall.SIGCONT)
		}
	}
	isPaused = false
}

// HandleTranscode returns a JobHandler that performs a background HLS transcode
// for a video media item. The transcode is stored under the item's data directory.
//
// Jobs are skipped (deleted) if:
//   - video_metadata.transcoded_at IS NOT NULL (already done)
//   - The file's bitrate is at or below cfg.TranscodeBitrateKbps (no benefit)
func HandleTranscode(cfg *config.Config) func(ctx context.Context, db utils.DBTX, job *models.Job) error {
	return func(ctx context.Context, db utils.DBTX, job *models.Job) error {
		if job.RelatedID == nil {
			return fmt.Errorf("transcode job %d has no related_id", job.ID)
		}
		itemID := *job.RelatedID

		// Fetch video metadata to check if transcode is still needed
		meta, err := repository.VideoMetadataForTranscode(ctx, db, itemID)
		if err != nil {
			return fmt.Errorf("fetch video_metadata for item %d: %w", itemID, err)
		}

		// Already transcoded — delete job and return
		if meta.TranscodedAt != nil {
			_ = repository.JobDelete(ctx, db, job.ID)
			slog.Info("already transcoded", "itemID", strconv.FormatInt(itemID, 10))
			return nil
		}

		// Bitrate at or below threshold — no benefit to transcoding
		if meta.BitrateKbps != nil && *meta.BitrateKbps <= cfg.TranscodeBitrateKbps {
			_ = repository.JobDelete(ctx, db, job.ID)
			slog.Info("transcode unnecessary", "itemID", strconv.FormatInt(itemID, 10))
			return nil
		}

		// Fetch file path
		relativePath, _, fileHash, err := repository.MediaItemForProcessing(ctx, db, itemID)
		if err != nil {
			return fmt.Errorf("fetch media item %d: %w", itemID, err)
		}
		fsPath := filepath.Join(cfg.MediaPath, relativePath)

		outDir := imaging.VideoHLSDir(cfg.DataPath, fileHash)
		if err := os.MkdirAll(outDir, 0o755); err != nil {
			return fmt.Errorf("mkdir hls dir: %w", err)
		}

		manifestPath := imaging.VideoHLSManifest(cfg.DataPath, fileHash)
		segPattern := filepath.Join(outDir, "seg_%05d.ts")

		bitrateStr := fmt.Sprintf("%dk", cfg.TranscodeBitrateKbps)
		bufsizeStr := fmt.Sprintf("%dk", cfg.TranscodeBitrateKbps*2)

		cmd := exec.Command("ffmpeg",
			"-i", fsPath,
			"-c:v", "libx264",
			"-preset", "slow",
			"-crf", "23",
			"-maxrate", bitrateStr,
			"-bufsize", bufsizeStr,
			"-c:a", "aac",
			"-b:a", "128k",
			"-hls_time", "6",
			"-hls_playlist_type", "vod",
			"-hls_segment_filename", segPattern,
			manifestPath,
		)

		// Run at lower OS scheduling priority (nice level 10 on Linux).
		setNice(cmd)

		activeMu.Lock()
		activeCmds[job.ID] = cmd

		_ = repository.JobUpdateProgress(ctx, db, job.ID, fmt.Sprintf("transcoding %s", fsPath))

		if err := cmd.Start(); err != nil {
			delete(activeCmds, job.ID)
			activeMu.Unlock()
			return fmt.Errorf("start ffmpeg transcode: %w", err)
		}

		// If the system is paused, pause this new process immediately.
		if isPaused {
			_ = cmd.Process.Signal(syscall.SIGSTOP)
		}

		activeMu.Unlock()
		waitErr := cmd.Wait()

		activeMu.Lock()
		delete(activeCmds, job.ID)
		activeMu.Unlock()

		if waitErr != nil {
			return fmt.Errorf("ffmpeg transcode: %w", waitErr)
		}

		// Mark transcoded_at
		if err := repository.VideoSetTranscodedAt(ctx, db, itemID, time.Now()); err != nil {
			slog.Warn("set transcoded_at", "item_id", itemID, "err", err)
		}

		slog.Info("transcode complete", "item_id", itemID)
		_ = repository.JobDelete(ctx, db, job.ID)
		return nil
	}
}
