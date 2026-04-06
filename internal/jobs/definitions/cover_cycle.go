package definitions

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

// HandleCoverCycle returns a job handler that cycles cover images.
// Path 1: for each enabled collection without manual_cover, picks a random item
// with generated variants and creates a square-cropped cover from its thumb.
// Path 2: for each artist without manual_image, picks a random non-compilation
// album cover and uses it as the artist image.
func HandleCoverCycle(dataPath string) func(ctx context.Context, db utils.DBTX, job *models.Job) error {
	return func(ctx context.Context, db utils.DBTX, job *models.Job) error {
		_ = repository.JobUpdateProgress(ctx, db, job.ID, "starting cover cycle")

		// Path 1: photo collection covers
		collIDs, err := repository.CollectionsWithNonManualCoverIDs(ctx, db)
		if err != nil {
			return fmt.Errorf("list collections: %w", err)
		}

		var updatedColls int
		for _, collID := range collIDs {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err := GenerateCoverForCollection(ctx, db, dataPath, collID); err != nil {
				slog.Warn("cover cycle: skip collection", "collection_id", collID, "err", err)
				continue
			}
			updatedColls++
		}

		_ = repository.JobUpdateProgress(ctx, db, job.ID,
			fmt.Sprintf("cycled %d/%d collection covers; starting artist images", updatedColls, len(collIDs)))

		// Path 2: music artist images
		artistIDs, err := repository.ArtistsWithoutManualImage(ctx, db)
		if err != nil {
			return fmt.Errorf("list artists: %w", err)
		}

		var updatedArtists int
		for _, artistID := range artistIDs {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err := GenerateCoverForArtist(ctx, db, dataPath, artistID); err != nil {
				slog.Warn("cover cycle: skip artist", "artist_id", artistID, "err", err)
				continue
			}
			updatedArtists++
		}

		_ = repository.JobUpdateProgress(ctx, db, job.ID,
			fmt.Sprintf("cycled %d/%d collection covers, %d/%d artist images",
				updatedColls, len(collIDs), updatedArtists, len(artistIDs)))

		return nil
	}
}

// GenerateCoverForArtist picks a random non-compilation album for the given artist,
// checks that an album cover AVIF exists on disk, and generates the artist image from it.
// Returns nil silently if no eligible album or cover exists.
func GenerateCoverForArtist(ctx context.Context, db utils.DBTX, dataPath string, artistID int64) error {
	albumID, err := repository.AlbumGetRandomNonCompilationIDByArtist(ctx, db, artistID)
	if err == pgx.ErrNoRows {
		return nil // no eligible albums
	}
	if err != nil {
		return fmt.Errorf("pick random album: %w", err)
	}

	if !imaging.AlbumCoverExists(dataPath, albumID) {
		return nil // album cover not generated yet
	}

	srcPath := imaging.AlbumCoverPath(dataPath, albumID, "avif")
	if err := imaging.GenerateArtistCover(srcPath, dataPath, artistID); err != nil {
		return fmt.Errorf("generate artist cover: %w", err)
	}

	return nil
}

// GenerateCoverForCollection picks a random item with variants from the
// given collection and generates a square-cropped cover image from its thumb.
// Returns nil if the collection has no eligible items (silently skips).
func GenerateCoverForCollection(ctx context.Context, db utils.DBTX, dataPath string, collectionID int64) error {
	hash, err := repository.MediaItemRandomHashWithVariants(ctx, db, collectionID)
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
