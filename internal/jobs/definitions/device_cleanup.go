package definitions

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/stevenvi/bokeh-mediaserver/internal/jobs"
	"github.com/stevenvi/bokeh-mediaserver/internal/repository"
)

// DeviceCleanupMeta describes the device_cleanup job type.
var DeviceCleanupMeta = jobs.JobMeta{
	Description: "Remove inactive device sessions",
}

const deviceStaleAge = 365 * 24 * time.Hour

// HandleDeviceCleanup returns a job handler that deletes non-banned devices
// not seen in the past year.
func HandleDeviceCleanup() jobs.JobHandler {
	return func(ctx context.Context, jc *jobs.JobContext) error {
		db, job := jc.DB, jc.Job
		_ = repository.JobUpdateProgress(ctx, db, job.ID, "starting device cleanup")

		cutoff := time.Now().Add(-deviceStaleAge)
		ids, err := repository.DevicesStaleNonBanned(ctx, db, cutoff)
		if err != nil {
			return fmt.Errorf("list stale devices: %w", err)
		}

		if len(ids) == 0 {
			_ = repository.JobUpdateProgress(ctx, db, job.ID, "no stale devices to remove")
			return nil
		}

		_ = repository.JobUpdateProgress(ctx, db, job.ID, fmt.Sprintf("removing %d stale devices", len(ids)))

		if err := repository.DevicesDeleteByID(ctx, db, ids); err != nil {
			return fmt.Errorf("delete stale devices: %w", err)
		}

		summary := fmt.Sprintf("device cleanup complete: %d stale devices removed", len(ids))
		_ = repository.JobUpdateProgress(ctx, db, job.ID, summary)
		slog.Info(summary)
		return nil
	}
}
