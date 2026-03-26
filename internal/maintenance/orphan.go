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

const orphanBatchSize = 256

// HandleOrphanCleanup returns a job handler that walks DATA_PATH hash-addressed
// directories and removes derived files for content hashes that no longer have a
// corresponding media_item in the database.
//
// Directory structure: {base}/{hash[0:2]}/{hash[2:4]}/{hash[4:]}/
// The full hash is reconstructed by concatenating the three directory name components.
func HandleOrphanCleanup(dataPath string) func(ctx context.Context, db utils.DBTX, job *models.Job) error {
	return func(ctx context.Context, db utils.DBTX, job *models.Job) error {
		jobRepo := repository.NewJobRepository(db)
		mediaRepo := repository.NewMediaItemRepository(db)

		_ = jobRepo.UpdateProgress(ctx, job.ID, "starting orphan cleanup")

		var cleaned, checked int64

		derivedPath := dataPath + "/derived_media"
		l1Entries, err := os.ReadDir(derivedPath)
		if err != nil {
			return fmt.Errorf("read data path: %w", err)
		}

		// Accumulate up to orphanBatchSize hashes before querying the DB.
		type entry struct {
			hash string
			path string
		}
		var batch []entry

		flush := func() error {
			if len(batch) == 0 {
				return nil
			}

			hashes := make([]string, len(batch))
			for i, e := range batch {
				hashes[i] = e.hash
			}

			existing, err := mediaRepo.FindHashesExisting(ctx, hashes)
			if err != nil {
				return fmt.Errorf("batch hash lookup: %w", err)
			}

			for _, e := range batch {
				checked++
				if _, ok := existing[e.hash]; !ok {
					if err := os.RemoveAll(e.path); err != nil {
						slog.Warn("remove orphan data", "hash", e.hash, "path", e.path, "err", err)
						continue
					}
					cleaned++
					slog.Debug("removed orphan data", "hash", e.hash)
				}
			}
			batch = batch[:0]
			return nil
		}

		for _, l1 := range l1Entries {
			if !l1.IsDir() || len(l1.Name()) != 2 {
				continue
			}
			l1Path := derivedPath + "/" + l1.Name()

			l2Entries, err := os.ReadDir(l1Path)
			if err != nil {
				slog.Warn("read l1 dir", "path", l1Path, "err", err)
				continue
			}

			for _, l2 := range l2Entries {
				if !l2.IsDir() || len(l2.Name()) != 2 {
					continue
				}
				l2Path := l1Path + "/" + l2.Name()

				l3Entries, err := os.ReadDir(l2Path)
				if err != nil {
					slog.Warn("read l2 dir", "path", l2Path, "err", err)
					continue
				}

				for _, l3 := range l3Entries {
					if ctx.Err() != nil {
						return ctx.Err()
					}
					if !l3.IsDir() {
						continue
					}
					// Leaf directory name is hash[4:] — 60 hex chars for BLAKE2b-256
					hash := l1.Name() + l2.Name() + l3.Name()
					leafPath := imaging.ItemDataPath(dataPath, hash)
					batch = append(batch, entry{hash: hash, path: leafPath})

					if len(batch) >= orphanBatchSize {
						if err := flush(); err != nil {
							return err
						}
					}
				}
			}
		}

		if err := flush(); err != nil {
			return err
		}

		summary := fmt.Sprintf("orphan cleanup complete: %d checked, %d removed", checked, cleaned)
		_ = jobRepo.UpdateProgress(ctx, job.ID, summary)
		slog.Info(summary)
		return nil
	}
}
