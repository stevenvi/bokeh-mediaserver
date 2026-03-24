package maintenance

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/stevenvi/bokeh-mediaserver/internal/imaging"
	"github.com/stevenvi/bokeh-mediaserver/internal/models"
	"github.com/stevenvi/bokeh-mediaserver/internal/repository"
	"github.com/stevenvi/bokeh-mediaserver/internal/utils"
)

// HandleCoverCycle returns a job handler that cycles collection cover images.
// For each enabled collection without manual_cover, it picks a random item
// with generated variants and creates a square-cropped cover from its thumb.
func HandleCoverCycle(dataPath string) func(ctx context.Context, db utils.DBTX, job *models.Job) error {
	return func(ctx context.Context, db utils.DBTX, job *models.Job) error {
		jobRepo := repository.NewJobRepository(db)
		collRepo := repository.NewCollectionRepository(db)

		_ = jobRepo.UpdateProgress(ctx, job.ID, "starting cover cycle")

		collIDs, err := collRepo.ListCollectionswithNonManualCoverIDs(ctx)
		if err != nil {
			return fmt.Errorf("list collections: %w", err)
		}

		var updated int
		for _, collID := range collIDs {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err := GenerateCoverForCollection(ctx, db, dataPath, collID); err != nil {
				slog.Warn("cover cycle: skip collection", "collection_id", collID, "err", err)
				continue
			}
			updated++
		}

		_ = jobRepo.UpdateProgress(ctx, job.ID, fmt.Sprintf("cycled %d/%d collection covers", updated, len(collIDs)))
		return nil
	}
}

// GenerateCoverForCollection picks a random item with variants from the
// given collection and generates a square-cropped cover image from its thumb.
// Returns nil if the collection has no eligible items (silently skips).
func GenerateCoverForCollection(ctx context.Context, db utils.DBTX, dataPath string, collectionID int64) error {
	mediaRepo := repository.NewMediaItemRepository(db)

	hash, err := mediaRepo.GetRandomItemHashWithVariants(ctx, collectionID)
	if err == pgx.ErrNoRows {
		return nil // no items with variants yet
	}
	if err != nil {
		return fmt.Errorf("pick random item: %w", err)
	}

	thumbPath := imaging.VariantPath(dataPath, hash, imaging.VariantThumb, "avif")
	if err := imaging.GenerateCollectionCover(thumbPath, dataPath, collectionID); err != nil {
		return fmt.Errorf("generate cover: %w", err)
	}

	return nil
}
