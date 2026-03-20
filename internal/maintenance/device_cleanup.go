package maintenance

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/stevenvi/bokeh-mediaserver/internal/models"
	"github.com/stevenvi/bokeh-mediaserver/internal/repository"
	"github.com/stevenvi/bokeh-mediaserver/internal/utils"
)

const deviceStaleAge = 365 * 24 * time.Hour

// HandleDeviceCleanup returns a job handler that deletes non-banned devices
// not seen in the past year.
func HandleDeviceCleanup() func(ctx context.Context, db utils.DBTX, job *models.Job) error {
	return func(ctx context.Context, db utils.DBTX, job *models.Job) error {
		deviceRepo := repository.NewDeviceRepository(db)
		jobRepo := repository.NewJobRepository(db)

		_ = jobRepo.UpdateProgress(ctx, job.ID, "starting device cleanup")

		cutoff := time.Now().Add(-deviceStaleAge)
		ids, err := deviceRepo.ListStaleNonBanned(ctx, cutoff)
		if err != nil {
			return fmt.Errorf("list stale devices: %w", err)
		}

		if len(ids) == 0 {
			_ = jobRepo.UpdateProgress(ctx, job.ID, "no stale devices to remove")
			return nil
		}

		_ = jobRepo.UpdateProgress(ctx, job.ID, fmt.Sprintf("removing %d stale devices", len(ids)))

		if err := deviceRepo.DeleteByIDs(ctx, ids); err != nil {
			return fmt.Errorf("delete stale devices: %w", err)
		}

		summary := fmt.Sprintf("device cleanup complete: %d stale devices removed", len(ids))
		_ = jobRepo.UpdateProgress(ctx, job.ID, summary)
		slog.Info(summary)
		return nil
	}
}
