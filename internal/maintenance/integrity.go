package maintenance

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/stevenvi/bokeh-mediaserver/internal/imaging"
	"github.com/stevenvi/bokeh-mediaserver/internal/models"
	"github.com/stevenvi/bokeh-mediaserver/internal/repository"
	"github.com/stevenvi/bokeh-mediaserver/internal/utils"
)

// HandleIntegrityCheck returns a job handler that:
// 1. Deletes media items missing for >90 days and cleans up their derived files
// 2. Queues process_media jobs for items with missing variants
func HandleIntegrityCheck(dataPath string) func(ctx context.Context, db utils.DBTX, job *models.Job) error {
	return func(ctx context.Context, db utils.DBTX, job *models.Job) error {
		jobRepo := repository.NewJobRepository(db)
		mediaRepo := repository.NewMediaItemRepository(db)

		_ = jobRepo.UpdateProgress(ctx, job.ID, "starting integrity check")

		// prune stale missing items (missing for >90 days)
		pruned, err := pruneStaleItems(ctx, mediaRepo, jobRepo, dataPath, job.ID)
		if err != nil {
			return fmt.Errorf("prune stale items: %w", err)
		}

		summary := fmt.Sprintf("integrity check complete: %d stale items pruned", pruned)
		_ = jobRepo.UpdateProgress(ctx, job.ID, summary)
		slog.Info(summary)
		return nil
	}
}

func pruneStaleItems(ctx context.Context, mediaRepo *repository.MediaItemRepository, jobRepo *repository.JobRepository, dataPath string, jobID int64) (int64, error) {
	items, err := mediaRepo.ListStaleItems(ctx)
	if err != nil {
		return 0, fmt.Errorf("query stale items: %w", err)
	}

	if len(items) == 0 {
		_ = jobRepo.UpdateProgress(ctx, jobID, "no stale items to prune")
		return 0, nil
	}

	_ = jobRepo.UpdateProgress(ctx, jobID,
		fmt.Sprintf("pruning %d stale items", len(items)))

	var pruned int64
	for _, item := range items {
		if ctx.Err() != nil {
			return pruned, ctx.Err()
		}

		// Delete from DB (cascades to photo_metadata)
		if err := mediaRepo.Delete(ctx, item.ID); err != nil {
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
