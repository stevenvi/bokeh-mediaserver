package definitions

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/stevenvi/bokeh-mediaserver/internal/imaging"
	"github.com/stevenvi/bokeh-mediaserver/internal/jobs"
	"github.com/stevenvi/bokeh-mediaserver/internal/repository"
	"github.com/stevenvi/bokeh-mediaserver/internal/utils"
)

// CoverCycleMeta describes the cover_cycle job type.
var CoverCycleMeta = jobs.JobMeta{
	Description: "Cycle auto-generated collection and artist thumbnails to bring some variety to your collection views",
}

// HandleCoverCycle returns a job handler that cycles cover images.
// Path 1: for each enabled collection without manual_thumbnail, picks a random item
// with generated variants and creates a square-cropped thumbnail from its thumb.
// Path 2: for each artist without manual_thumbnail, picks a random non-compilation
// album thumbnail and uses it as the artist thumbnail.
func HandleCoverCycle(dataPath string) jobs.JobHandler {
	return func(ctx context.Context, jc *jobs.JobContext) error {
		db, job := jc.DB, jc.Job
		_ = repository.JobUpdateProgress(ctx, db, job.ID, "starting cover cycle")

		// Path 1: photo collection thumbnails
		collIDs, err := repository.CollectionsWithNonManualThumbnailIDs(ctx, db)
		if err != nil {
			return fmt.Errorf("list collections: %w", err)
		}

		var updatedColls int
		for _, collID := range collIDs {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err := GenerateThumbnailForCollection(ctx, db, dataPath, collID); err != nil {
				slog.Warn("cover cycle: skip collection", "collection_id", collID, "err", err)
				continue
			}
			updatedColls++
		}

		_ = repository.JobUpdateProgress(ctx, db, job.ID,
			fmt.Sprintf("cycled %d/%d collection thumbnails; starting artist thumbnails", updatedColls, len(collIDs)))

		// Path 2: music artist thumbnails
		artistIDs, err := repository.ArtistsWithoutManualThumbnail(ctx, db)
		if err != nil {
			return fmt.Errorf("list artists: %w", err)
		}

		var updatedArtists int
		for _, artistID := range artistIDs {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err := GenerateThumbnailForArtist(ctx, db, dataPath, artistID); err != nil {
				slog.Warn("cover cycle: skip artist", "artist_id", artistID, "err", err)
				continue
			}
			updatedArtists++
		}

		_ = repository.JobUpdateProgress(ctx, db, job.ID,
			fmt.Sprintf("cycled %d/%d collection thumbnails, %d/%d artist thumbnails",
				updatedColls, len(collIDs), updatedArtists, len(artistIDs)))

		return nil
	}
}

// GenerateThumbnailForArtist picks a random non-compilation album for the given artist,
// checks that an album thumbnail AVIF exists on disk, and generates the artist thumbnail from it.
// Returns nil silently if no eligible album or thumbnail exists.
func GenerateThumbnailForArtist(ctx context.Context, db utils.DBTX, dataPath string, artistID int64) error {
	albumID, err := repository.AlbumGetRandomNonCompilationIDByArtist(ctx, db, artistID, 0)
	if err == pgx.ErrNoRows {
		return nil // no eligible albums
	}
	if err != nil {
		return fmt.Errorf("pick random album: %w", err)
	}

	if !imaging.AlbumThumbnailExists(dataPath, albumID) {
		return nil // album thumbnail not generated yet
	}

	srcPath := imaging.AlbumThumbnailPath(dataPath, albumID, "avif")
	if err := imaging.GenerateArtistThumbnail(srcPath, dataPath, artistID); err != nil {
		return fmt.Errorf("generate artist thumbnail: %w", err)
	}

	return nil
}

// GenerateThumbnailForCollection picks a random item with variants from the
// given collection and generates a square-cropped thumbnail image from its thumb.
// Returns nil if the collection has no eligible items (silently skips).
func GenerateThumbnailForCollection(ctx context.Context, db utils.DBTX, dataPath string, collectionID int64) error {
	hash, err := repository.MediaItemRandomHashWithVariants(ctx, db, collectionID)
	if err == pgx.ErrNoRows {
		return nil // no items with variants yet
	}
	if err != nil {
		return fmt.Errorf("pick random item: %w", err)
	}

	thumbPath := imaging.VariantPath(dataPath, hash, imaging.VariantThumb, "avif")
	if err := imaging.GenerateCollectionThumbnail(thumbPath, dataPath, collectionID); err != nil {
		return fmt.Errorf("generate thumbnail: %w", err)
	}

	return nil
}
