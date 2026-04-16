package definitions

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/stevenvi/bokeh-mediaserver/internal/imaging"
	"github.com/stevenvi/bokeh-mediaserver/internal/jobs"
	"github.com/stevenvi/bokeh-mediaserver/internal/repository"
	"github.com/stevenvi/bokeh-mediaserver/internal/utils"
)

// IntegrityCheckMeta describes the integrity_check job type.
var IntegrityCheckMeta = jobs.JobMeta{
	Description: "Prune stale items and check for missing files",
}

// HandleIntegrityCheck returns a job handler that:
// 1. Deletes media items missing for >90 days and cleans up their derived files
// 2. Re-queues video_transcode for video items whose HLS manifest was deleted on disk
func HandleIntegrityCheck(dataPath string, dispatcher *jobs.Dispatcher) jobs.JobHandler {
	return func(ctx context.Context, jc *jobs.JobContext) error {
		db, job := jc.DB, jc.Job
		_ = repository.JobUpdateProgress(ctx, db, job.ID, "starting integrity check")

		// prune stale missing items (missing for >90 days)
		pruned, err := pruneStaleItems(ctx, db, dataPath, job.ID)
		if err != nil {
			return fmt.Errorf("prune stale items: %w", err)
		}

		// check video derivatives (re-queue transcode/process_media for broken outputs)
		requeued, err := checkVideoDerivatives(ctx, db, dataPath, job.ID)
		if err != nil {
			slog.Warn("check video derivatives failed", "err", err)
		}
		if requeued > 0 {
			dispatcher.TriggerImmediately()
		}

		summary := fmt.Sprintf("integrity check complete: %d stale items pruned, %d video jobs re-queued", pruned, requeued)
		_ = repository.JobUpdateProgress(ctx, db, job.ID, summary)
		slog.Info(summary)
		return nil
	}
}

// checkVideoDerivatives iterates video_metadata rows and re-queues jobs when
// the expected output files are missing from disk.
func checkVideoDerivatives(ctx context.Context, db utils.DBTX, dataPath string, jobID int64) (int64, error) {
	_ = repository.JobUpdateProgress(ctx, db, jobID, "checking video derivatives")

	items, err := repository.VideosForIntegrityCheck(ctx, db)
	if err != nil {
		return 0, fmt.Errorf("query video_metadata: %w", err)
	}

	var requeued int64
	relatedType := "media_item"

	for _, item := range items {
		if ctx.Err() != nil {
			break
		}

		// Re-queue video_transcode if transcoded_at is set but the manifest is gone
		if item.TranscodedAt != nil {
			manifestPath := imaging.VideoHLSManifest(dataPath, item.FileHash)
			if _, statErr := os.Stat(manifestPath); os.IsNotExist(statErr) {
				// Clear transcoded_at so the transcoder will run again
				if err := repository.VideoClearTranscodedAt(ctx, db, item.ItemID); err != nil {
					slog.Warn("clear transcoded_at", "item_id", item.ItemID, "err", err)
					continue
				}
				active, _ := repository.JobIsActive(ctx, db, "video_transcode_item", item.ItemID)
				if !active {
					if _, err := repository.JobCreate(ctx, db, "video_transcode_item", &item.ItemID, &relatedType, nil); err != nil {
						slog.Warn("re-queue video_transcode_item", "item_id", item.ItemID, "err", err)
					} else {
						requeued++
					}
				}
			}
		}
	}

	return requeued, nil
}

func pruneStaleItems(ctx context.Context, db utils.DBTX, dataPath string, jobID int64) (int64, error) {
	items, err := repository.MediaItemsStale(ctx, db)
	if err != nil {
		return 0, fmt.Errorf("query stale items: %w", err)
	}

	if len(items) == 0 {
		_ = repository.JobUpdateProgress(ctx, db, jobID, "no stale items to prune")
		return 0, nil
	}

	_ = repository.JobUpdateProgress(ctx, db, jobID,
		fmt.Sprintf("pruning %d stale items", len(items)))

	var pruned int64
	for _, item := range items {
		if ctx.Err() != nil {
			return pruned, ctx.Err()
		}

		// Delete from DB (cascades to photo_metadata)
		if err := repository.DeleteMediaItem(ctx, db, item.ID); err != nil {
			slog.Warn("delete stale media item", "item_id", item.ID, "err", err)
			continue
		}

		// Clean up derived files on disk
		itemPath := imaging.ItemDataPath(dataPath, item.Hash)
		if err := os.RemoveAll(itemPath); err != nil {
			slog.Warn("remove stale item data", "item_id", item.ID, "hash", item.Hash, "path", itemPath, "err", err)
		}

		pruned++
	}

	return pruned, nil
}
