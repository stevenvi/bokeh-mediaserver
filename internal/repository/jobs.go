package repository

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/stevenvi/bokeh-mediaserver/internal/models"
	"github.com/stevenvi/bokeh-mediaserver/internal/utils"
)

// JobClaimNext atomically claims the next queued job of one of the given types.
// Returns nil, nil if no job is available. Uses SKIP LOCKED to avoid contention.
func JobClaimNext(ctx context.Context, db utils.DBTX, jobTypes []string) (*models.Job, error) {
	var j models.Job
	err := db.QueryRow(ctx,
		`UPDATE jobs SET status = 'running', started_at = now()
		 WHERE id = (
		     SELECT id FROM jobs
		     WHERE status = 'queued' AND type = ANY($1)
		     ORDER BY queued_at
		     LIMIT 1
		     FOR UPDATE SKIP LOCKED
		 ) RETURNING id, type, status, related_id, related_type,
		             log, error_message,
		             queued_at, started_at, completed_at`,
		jobTypes,
	).Scan(
		&j.ID, &j.Type, &j.Status, &j.RelatedID, &j.RelatedType,
		&j.Log, &j.ErrorMessage,
		&j.QueuedAt, &j.StartedAt, &j.CompletedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &j, nil
}

// JobCreate inserts a new job row and returns its ID.
func JobCreate(ctx context.Context, db utils.DBTX, jobType string, relatedID *int64, relatedType *string) (int64, error) {
	var id int64
	err := db.QueryRow(ctx,
		`INSERT INTO jobs (type, related_id, related_type)
		 VALUES ($1, $2, $3)
		 RETURNING id`,
		jobType, relatedID, relatedType,
	).Scan(&id)
	return id, err
}

// JobMarkRunning sets a job to running status with the current timestamp.
func JobMarkRunning(ctx context.Context, db utils.DBTX, jobID int64) error {
	_, err := db.Exec(ctx,
		`UPDATE jobs SET status = 'running', started_at = now() WHERE id = $1`,
		jobID,
	)
	return err
}

// JobUpdateProgress appends a line to the job's log.
func JobUpdateProgress(ctx context.Context, db utils.DBTX, jobID int64, msg string) error {
	_, err := db.Exec(ctx,
		`UPDATE jobs
		 SET log = COALESCE(log, '') || $2
		 WHERE id = $1`,
		jobID, msg+"\n",
	)
	return err
}

// JobMarkDone sets a job to done status.
func JobMarkDone(ctx context.Context, db utils.DBTX, jobID int64) error {
	_, err := db.Exec(ctx,
		`UPDATE jobs
		 SET status = 'done', completed_at = now()
		 WHERE id = $1`,
		jobID,
	)
	return err
}

// JobMarkFailed sets a job to failed status with an error message.
func JobMarkFailed(ctx context.Context, db utils.DBTX, jobID int64, errMsg string) error {
	_, err := db.Exec(ctx,
		`UPDATE jobs
		 SET status = 'failed', error_message = $2, completed_at = now()
		 WHERE id = $1`,
		jobID, errMsg,
	)
	return err
}

// JobDelete removes a job row.
func JobDelete(ctx context.Context, db utils.DBTX, jobID int64) error {
	_, err := db.Exec(ctx, `DELETE FROM jobs WHERE id = $1`, jobID)
	return err
}

// JobGet returns a single job by ID.
func JobGet(ctx context.Context, db utils.DBTX, jobID int64) (*models.Job, error) {
	var j models.Job
	err := db.QueryRow(ctx,
		`SELECT id, type, status, related_id, related_type,
		        log, error_message,
		        queued_at, started_at, completed_at
		 FROM jobs WHERE id = $1`,
		jobID,
	).Scan(
		&j.ID, &j.Type, &j.Status, &j.RelatedID, &j.RelatedType,
		&j.Log, &j.ErrorMessage,
		&j.QueuedAt, &j.StartedAt, &j.CompletedAt,
	)
	if err != nil {
		return nil, err
	}
	return &j, nil
}

// JobIsActive returns true if a job of the given type is queued or running for the given related ID.
func JobIsActive(ctx context.Context, db utils.DBTX, jobType string, relatedID int64) (bool, error) {
	var count int
	err := db.QueryRow(ctx,
		`SELECT COUNT(*) FROM jobs
		 WHERE type = $1 AND related_id = $2 AND status IN ('queued', 'running')`,
		jobType, relatedID,
	).Scan(&count)
	return count > 0, err
}

// JobIsAnyActive returns true if any job matching one of jobTypes is queued or
// running for the given related ID. Used to enforce per-collection scan concurrency.
func JobIsAnyActive(ctx context.Context, db utils.DBTX, jobTypes []string, relatedID int64) (bool, error) {
	var count int
	err := db.QueryRow(ctx,
		`SELECT COUNT(*) FROM jobs
		 WHERE type = ANY($1) AND related_id = $2 AND status IN ('queued', 'running')`,
		jobTypes, relatedID,
	).Scan(&count)
	return count > 0, err
}

// JobIsActiveByType returns true if any job of the given type is queued or running.
func JobIsActiveByType(ctx context.Context, db utils.DBTX, jobType string) (bool, error) {
	var count int
	err := db.QueryRow(ctx,
		`SELECT COUNT(*) FROM jobs
		 WHERE type = $1 AND status IN ('queued', 'running')`,
		jobType,
	).Scan(&count)
	return count > 0, err
}

// JobsResetStuck resets any jobs left in 'running' state back to 'queued'.
func JobsResetStuck(ctx context.Context, db utils.DBTX) error {
	tag, err := db.Exec(ctx,
		`UPDATE jobs SET status = 'queued', started_at = NULL
		 WHERE status = 'running'`,
	)
	if err != nil {
		return fmt.Errorf("recover stuck jobs: %w", err)
	}
	if tag.RowsAffected() > 0 {
		slog.Warn("recovered stuck jobs from previous run",
			"count", tag.RowsAffected())
	}
	return nil
}
