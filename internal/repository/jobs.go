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

type JobRepository struct {
	db utils.DBTX
}

func NewJobRepository(db utils.DBTX) *JobRepository {
	return &JobRepository{db: db}
}

// ClaimNext atomically claims the next queued job of one of the given types.
// Returns nil, nil if no job is available. Uses SKIP LOCKED to avoid contention.
func (r *JobRepository) ClaimNext(ctx context.Context, jobTypes []string) (*models.Job, error) {
	var j models.Job
	err := r.db.QueryRow(ctx,
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

// Create inserts a new job row and returns its ID.
func (r *JobRepository) Create(ctx context.Context, jobType string, relatedID *int64, relatedType *string) (int64, error) {
	var id int64
	err := r.db.QueryRow(ctx,
		`INSERT INTO jobs (type, related_id, related_type)
		 VALUES ($1, $2, $3)
		 RETURNING id`,
		jobType, relatedID, relatedType,
	).Scan(&id)
	return id, err
}

// MarkRunning sets a job to running status with the current timestamp.
func (r *JobRepository) MarkRunning(ctx context.Context, jobID int64) error {
	_, err := r.db.Exec(ctx,
		`UPDATE jobs SET status = 'running', started_at = now() WHERE id = $1`,
		jobID,
	)
	return err
}

// UpdateProgress appends a line to the job's log.
func (r *JobRepository) UpdateProgress(ctx context.Context, jobID int64, msg string) error {
	_, err := r.db.Exec(ctx,
		`UPDATE jobs
		 SET log = COALESCE(log, '') || $2
		 WHERE id = $1`,
		jobID, msg+"\n",
	)
	return err
}

// MarkDone sets a job to done status.
func (r *JobRepository) MarkDone(ctx context.Context, jobID int64) error {
	_, err := r.db.Exec(ctx,
		`UPDATE jobs
		 SET status = 'done', completed_at = now()
		 WHERE id = $1`,
		jobID,
	)
	return err
}

// MarkFailed sets a job to failed status with an error message.
func (r *JobRepository) MarkFailed(ctx context.Context, jobID int64, errMsg string) error {
	_, err := r.db.Exec(ctx,
		`UPDATE jobs
		 SET status = 'failed', error_message = $2, completed_at = now()
		 WHERE id = $1`,
		jobID, errMsg,
	)
	return err
}

// Delete removes a job row.
func (r *JobRepository) Delete(ctx context.Context, jobID int64) error {
	_, err := r.db.Exec(ctx, `DELETE FROM jobs WHERE id = $1`, jobID)
	return err
}

// GetByID returns a single job by ID.
func (r *JobRepository) GetByID(ctx context.Context, jobID int64) (*models.Job, error) {
	var j models.Job
	err := r.db.QueryRow(ctx,
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

// IsActiveByType returns true if any job of the given type is queued or running.
func (r *JobRepository) IsActiveByType(ctx context.Context, jobType string) (bool, error) {
	var count int
	err := r.db.QueryRow(ctx,
		`SELECT COUNT(*) FROM jobs
		 WHERE type = $1 AND status IN ('queued', 'running')`,
		jobType,
	).Scan(&count)
	return count > 0, err
}

// IsActive returns true if a job of the given type is queued or running for the given related ID.
func (r *JobRepository) IsActive(ctx context.Context, jobType string, relatedID int64) (bool, error) {
	var count int
	err := r.db.QueryRow(ctx,
		`SELECT COUNT(*) FROM jobs
		 WHERE type = $1 AND related_id = $2 AND status IN ('queued', 'running')`,
		jobType, relatedID,
	).Scan(&count)
	return count > 0, err
}

// RecoverStuck resets any jobs left in 'running' state back to 'queued'.
func (r *JobRepository) RecoverStuck(ctx context.Context) error {
	tag, err := r.db.Exec(ctx,
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

// LoadTranscodeBitrate reads the transcode_bitrate_kbps value from server_config.
func (r *JobRepository) LoadTranscodeBitrate(ctx context.Context) (int, error) {
	var kbps int
	err := r.db.QueryRow(ctx,
		`SELECT transcode_bitrate_kbps FROM server_config WHERE id = 1`,
	).Scan(&kbps)
	return kbps, err
}

// LoadSchedules reads cron schedules from server_config. Returns a map of
// config column name → nullable schedule string.
func (r *JobRepository) LoadSchedules(ctx context.Context) (map[string]*string, error) {
	var scanSched, integritySched, deviceCleanupSched, coverCycleSched *string
	err := r.db.QueryRow(ctx,
		`SELECT scan_schedule, integrity_schedule, device_cleanup_schedule, cover_cycle_schedule FROM server_config WHERE id = 1`,
	).Scan(&scanSched, &integritySched, &deviceCleanupSched, &coverCycleSched)
	if err != nil {
		return nil, err
	}
	return map[string]*string{
		"scan_schedule":           scanSched,
		"integrity_schedule":      integritySched,
		"device_cleanup_schedule": deviceCleanupSched,
		"cover_cycle_schedule":    coverCycleSched,
	}, nil
}
