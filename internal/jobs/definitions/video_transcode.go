package definitions

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"time"

	"github.com/stevenvi/bokeh-mediaserver/internal/config"
	"github.com/stevenvi/bokeh-mediaserver/internal/imaging"
	"github.com/stevenvi/bokeh-mediaserver/internal/jobs"
	jobsutils "github.com/stevenvi/bokeh-mediaserver/internal/jobs/utils"
	"github.com/stevenvi/bokeh-mediaserver/internal/repository"
)

// VideoTranscodeMeta describes the video_transcode parent job type.
var VideoTranscodeMeta = jobs.JobMeta{
	Description:     "Transcode videos to HLS format",
	SupportsSubjobs: true,
	MaxConcurrency:  1,
}

// VideoTranscodeItemMeta describes the video_transcode_item sub-job type.
var VideoTranscodeItemMeta = jobs.JobMeta{
	Description: "Transcode a single video to HLS format",
	TotalSteps:  1,
}

// HandleVideoTranscode returns a no-op handler for the video_transcode parent job.
// The actual work is done by HandleVideoTranscodeItem sub-jobs.
func HandleVideoTranscode() jobs.JobHandler {
	return func(ctx context.Context, jc *jobs.JobContext) error {
		// No-op: this is a container job. Sub-jobs do the actual transcoding.
		return nil
	}
}

// HandleVideoTranscodeItem returns a job handler that transcodes a single video
// to HLS format using ffmpeg at nice=19 (idle priority).
func HandleVideoTranscodeItem(cfg *config.Config) jobs.JobHandler {
	return func(ctx context.Context, jc *jobs.JobContext) error {
		db, job := jc.DB, jc.Job
		if job.RelatedID == nil {
			return fmt.Errorf("video_transcode_item job %d has no related_id", job.ID)
		}
		itemID := *job.RelatedID

		// Fetch video metadata to check if transcode is still needed
		meta, err := repository.VideoMetadataForTranscode(ctx, db, itemID)
		if err != nil {
			return fmt.Errorf("fetch video_metadata for item %d: %w", itemID, err)
		}

		// Already transcoded — skip
		// TODO: If we queued it to be transcoded, doesn't that fact suggest we don't care about the already-existing transcode?
		// For example, perhaps it is an incomplete transcode that we need to start over on
		if meta.TranscodedAt != nil {
			slog.Info("already transcoded", "itemID", strconv.FormatInt(itemID, 10))
			return nil
		}

		// Bitrate at or below threshold — no benefit to transcoding
		// TODO: Again, we should do this check _before_ queueing an item to be transcoded
		// It should only end up in this code path if we know that we actually want the
		// item to be transcoded.
		if meta.BitrateKbps != nil && *meta.BitrateKbps <= cfg.TranscodeBitrateKbps {
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

		// Run at idle OS scheduling priority (nice level 19).
		jobsutils.SetNice(cmd, 19)

		slog.Info("transcoding", "item_id", itemID, "path", fsPath)

		if err := cmd.Start(); err != nil {
			return fmt.Errorf("start ffmpeg transcode: %w", err)
		}

		waitErr := cmd.Wait()
		if waitErr != nil {
			return fmt.Errorf("ffmpeg transcode: %w", waitErr)
		}

		// Mark transcoded_at
		if err := repository.VideoSetTranscodedAt(ctx, db, itemID, time.Now()); err != nil {
			slog.Warn("set transcoded_at", "item_id", itemID, "err", err)
		}

		slog.Info("transcode complete", "item_id", itemID)
		return nil
	}
}
