package maintenance

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/stevenvi/bokeh-mediaserver/internal/imaging"
	"github.com/stevenvi/bokeh-mediaserver/internal/jobs"
	"github.com/stevenvi/bokeh-mediaserver/internal/models"
	"github.com/stevenvi/bokeh-mediaserver/internal/repository"
	"github.com/stevenvi/bokeh-mediaserver/internal/utils"
)

// HandleIntegrityCheck returns a job handler that:
// 1. Deletes media items missing for >90 days and cleans up their derived files
// 2. Re-queues transcode for video items whose HLS manifest was deleted on disk
// 3. Re-queues process_media for home movie items whose cover is missing
func HandleIntegrityCheck(dataPath string, dispatcher *jobs.Dispatcher) func(ctx context.Context, db utils.DBTX, job *models.Job) error {
	return func(ctx context.Context, db utils.DBTX, job *models.Job) error {
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

		// Re-queue transcode if transcoded_at is set but the manifest is gone
		if item.TranscodedAt != nil {
			manifestPath := imaging.VideoHLSManifest(dataPath, item.FileHash)
			if _, statErr := os.Stat(manifestPath); os.IsNotExist(statErr) {
				// Clear transcoded_at so the transcoder will run again
				if err := repository.VideoClearTranscodedAt(ctx, db, item.ItemID); err != nil {
					slog.Warn("clear transcoded_at", "item_id", item.ItemID, "err", err)
					continue
				}
				active, _ := repository.JobIsActive(ctx, db, "transcode", item.ItemID)
				if !active {
					if _, err := repository.JobCreate(ctx, db, "transcode", &item.ItemID, &relatedType); err != nil {
						slog.Warn("re-queue transcode", "item_id", item.ItemID, "err", err)
					} else {
						requeued++
					}
				}
			}
		}

		// Re-queue process_media for home movies whose cover is missing
		if item.CollectionType == "video:home_movie" {
			coverPath := imaging.VariantPath(dataPath, item.FileHash, "cover", "avif")
			if _, statErr := os.Stat(coverPath); os.IsNotExist(statErr) {
				active, _ := repository.JobIsActive(ctx, db, "process_media", item.ItemID)
				if !active {
					if _, err := repository.JobCreate(ctx, db, "process_media", &item.ItemID, &relatedType); err != nil {
						slog.Warn("re-queue process_media for cover", "item_id", item.ItemID, "err", err)
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
