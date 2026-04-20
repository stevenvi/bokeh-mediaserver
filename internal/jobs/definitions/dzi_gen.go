package definitions

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"

	"github.com/stevenvi/bokeh-mediaserver/internal/imaging"
	"github.com/stevenvi/bokeh-mediaserver/internal/jobs"
	"github.com/stevenvi/bokeh-mediaserver/internal/repository"
)

// DZIGenMeta describes the dzi_gen job type.
var DZIGenMeta = jobs.JobMeta{
	Description:     "Generate DZI tiles for a collection",
	TotalSteps:      1,
	SupportsSubjobs: true,
}

// HandleDZIGen returns a job handler that enqueues dzi_gen_item sub-jobs
// for all photo items in a collection (and its descendants) that lack DZI tiles.
func HandleDZIGen(mediaPath, dataPath string) jobs.JobHandler {
	return func(ctx context.Context, jc *jobs.JobContext) error {
		db, job := jc.DB, jc.Job
		if job.RelatedID == nil {
			return fmt.Errorf("dzi_gen job %d has no related_id", job.ID)
		}
		collectionID := *job.RelatedID

		jc.SetStep(ctx, 1)
		_ = repository.JobUpdateProgress(ctx, db, job.ID, "Enqueuing DZI generation sub-jobs")

		itemIDs, err := repository.PhotoItemIDsByCollection(ctx, db, collectionID)
		if err != nil {
			return fmt.Errorf("list photo items: %w", err)
		}

		var queued int
		relatedType := "media_item"
		for _, itemID := range itemIDs {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			id := itemID
			jc.AddSubJob("dzi_gen_item", &id, &relatedType)
			queued++
		}

		slog.Info("dzi_gen: enqueued sub-jobs",
			"job_id", job.ID,
			"collection_id", collectionID,
			"queued", queued,
		)
		return nil
	}
}

// DZIGenItemMeta describes the dzi_gen_item sub-job type.
var DZIGenItemMeta = jobs.JobMeta{
	Description: "Generate DZI tiles for a single photo",
	TotalSteps:  1,
}

// HandleDZIGenItem returns a job handler that generates DZI tiles for a single photo item.
func HandleDZIGenItem(mediaPath, dataPath string) jobs.JobHandler {
	return func(ctx context.Context, jc *jobs.JobContext) error {
		db, job := jc.DB, jc.Job
		if job.RelatedID == nil {
			return fmt.Errorf("dzi_gen_item job %d has no related_id", job.ID)
		}
		itemID := *job.RelatedID

		relativePath, _, fileHash, err := repository.MediaItemForProcessing(ctx, db, itemID)
		if err != nil {
			return fmt.Errorf("fetch media item %d: %w", itemID, err)
		}

		fsPath := filepath.Join(mediaPath, relativePath)

		generated, err := imaging.GenerateDZIIfNotPresent(fsPath, dataPath, fileHash)
		if err != nil {
			return fmt.Errorf("generate DZI: %w", err)
		}

		if generated {
			slog.Debug("dzi_gen_item: generated tiles", "item_id", itemID)
		} else {
			slog.Debug("dzi_gen_item: tiles already exist", "item_id", itemID)
		}

		return nil
	}
}
