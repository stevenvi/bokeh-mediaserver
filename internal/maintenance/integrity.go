package maintenance

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/stevenvi/bokeh-mediaserver/internal/utils"
	"github.com/stevenvi/bokeh-mediaserver/internal/imaging"
	"github.com/stevenvi/bokeh-mediaserver/internal/jobs"
	"github.com/stevenvi/bokeh-mediaserver/internal/models"
)

// HandleIntegrityCheck returns a job handler that:
// 1. Deletes media items missing for >90 days and cleans up their derived files
// 2. Queues process_media jobs for items with missing variants
func HandleIntegrityCheck(dataPath string) jobs.JobHandler {
	return func(ctx context.Context, db utils.DBTX, job *models.Job) error {
		_ = jobs.UpdateProgress(ctx, db, job.ID, "starting integrity check")

		// Phase 1 — prune stale missing items (missing for >90 days)
		pruned, err := pruneStaleItems(ctx, db, dataPath, job.ID)
		if err != nil {
			return fmt.Errorf("prune stale items: %w", err)
		}

		// Phase 2 — re-queue items with missing variants
		requeued, err := requeueMissingVariants(ctx, db, job.ID)
		if err != nil {
			return fmt.Errorf("requeue missing variants: %w", err)
		}

		summary := fmt.Sprintf("integrity check complete: %d stale items pruned, %d items re-queued for processing", pruned, requeued)
		_ = jobs.UpdateProgress(ctx, db, job.ID, summary)
		slog.Info(summary)
		return nil
	}
}

type staleItem struct {
	id   int64
	hash string
}

func pruneStaleItems(ctx context.Context, db utils.DBTX, dataPath string, jobID int64) (int64, error) {
	// Find items missing for >90 days; fetch hash so we can clean up the derived-data directory.
	rows, err := db.Query(ctx,
		`SELECT id, file_hash FROM media_items
		 WHERE missing_since IS NOT NULL
		   AND missing_since < now() - interval '90 days'`,
	)
	if err != nil {
		return 0, fmt.Errorf("query stale items: %w", err)
	}

	var items []staleItem
	for rows.Next() {
		var item staleItem
		if err := rows.Scan(&item.id, &item.hash); err != nil {
			continue
		}
		items = append(items, item)
	}
	rows.Close()

	if len(items) == 0 {
		_ = jobs.UpdateProgress(ctx, db, jobID, "no stale items to prune")
		return 0, nil
	}

	_ = jobs.UpdateProgress(ctx, db, jobID,
		fmt.Sprintf("pruning %d stale items", len(items)))

	var pruned int64
	for _, item := range items {
		if ctx.Err() != nil {
			return pruned, ctx.Err()
		}

		// Delete from DB (cascades to photo_metadata)
		_, err := db.Exec(ctx, `DELETE FROM media_items WHERE id = $1`, item.id)
		if err != nil {
			slog.Warn("delete stale media item", "item_id", item.id, "err", err)
			continue
		}

		// Clean up derived files on disk
		itemPath := imaging.ItemDataPath(dataPath, item.hash)
		if err := os.RemoveAll(itemPath); err != nil {
			slog.Warn("remove stale item data", "item_id", item.id, "hash", item.hash, "path", itemPath, "err", err)
		}

		pruned++
	}

	return pruned, nil
}

func requeueMissingVariants(ctx context.Context, db utils.DBTX, jobID int64) (int64, error) {
	// Find non-missing items with no generated variants
	// TODO: I'm not convinced this case is ever actually possible, the jobs should still be waiting in the queue is the data isn't generated
	// todo: we should remove the variants_generated_at column and nuke this code path

	return 0, nil // --- IGNORE ---
	
	rows, err := db.Query(ctx,
		`SELECT pm.media_item_id FROM photo_metadata pm
		 JOIN media_items mi ON mi.id = pm.media_item_id
		 WHERE pm.variants_generated_at IS NULL
		   AND mi.missing_since IS NULL`,
	)
	if err != nil {
		return 0, fmt.Errorf("query missing variants: %w", err)
	}

	// Collect all IDs before processing to avoid "conn busy" on single-connection
	// transactions (pgx.Tx uses one connection; can't run queries while rows are open).
	var itemIDs []int64
	for rows.Next() {
		var itemID int64
		if err := rows.Scan(&itemID); err != nil {
			continue
		}
		itemIDs = append(itemIDs, itemID)
	}
	rows.Close()

	var requeued int64
	for _, itemID := range itemIDs {
		if ctx.Err() != nil {
			return requeued, ctx.Err()
		}

		// Skip if a process_media job is already active for this item
		active, err := jobs.IsActive(ctx, db, "process_media", itemID)
		if err != nil {
			slog.Warn("check active process_media", "item_id", itemID, "err", err)
			continue
		}
		if active {
			continue
		}

		relatedType := "media_item"
		_, err = jobs.Create(ctx, db, "process_media", &itemID, &relatedType)
		if err != nil {
			slog.Warn("create process_media job", "item_id", itemID, "err", err)
			continue
		}
		requeued++
	}

	if requeued > 0 {
		_ = jobs.UpdateProgress(ctx, db, jobID,
			fmt.Sprintf("re-queued %d items for processing", requeued))
	}

	return requeued, nil
}
