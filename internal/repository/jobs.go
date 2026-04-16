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


// nonTerminalStatuses are the job statuses that indicate a job is still in progress.
var nonTerminalStatuses = []string{"queued", "running", "running_sub_jobs"}

// JobCreate inserts a new job row and returns its ID.
func JobCreate(ctx context.Context, db utils.DBTX, jobType string, relatedID *int64, relatedType *string, parentJobID *int64) (int64, error) {
	var id int64
	err := db.QueryRow(ctx,
		`INSERT INTO jobs (type, related_id, related_type, parent_job_id)
		 VALUES ($1, $2, $3, $4)
		 RETURNING id`,
		jobType, relatedID, relatedType, parentJobID,
	).Scan(&id)
	return id, err
}

// JobCreateSubJob inserts a sub-job and atomically increments the parent's
// subjobs_enqueued counter in a single statement.
func JobCreateSubJob(ctx context.Context, db utils.DBTX, jobType string, relatedID *int64, relatedType *string, parentID int64) (int64, error) {
	var id int64
	err := db.QueryRow(ctx,
		`WITH parent_bump AS (
		     UPDATE jobs SET subjobs_enqueued = subjobs_enqueued + 1 WHERE id = $5
		 )
		 INSERT INTO jobs (type, related_id, related_type, parent_job_id)
		 VALUES ($1, $2, $3, $4)
		 RETURNING id`,
		jobType, relatedID, relatedType, parentID, parentID,
	).Scan(&id)
	return id, err
}

// JobCreateSubJobBatch inserts multiple sub-jobs in a single statement and
// atomically increments the parent's subjobs_enqueued counter by the batch size.
// Returns the number of rows inserted.
func JobCreateSubJobBatch(ctx context.Context, db utils.DBTX, parentID int64, specs []SubJobSpec) (int, error) {
	if len(specs) == 0 {
		return 0, nil
	}

	// Build a multi-row VALUES clause: ($1, $2, $3, $4), ($5, $6, $7, $8), ...
	// Using unnest with arrays is more efficient for pgx.
	types := make([]string, len(specs))
	relatedIDs := make([]*int64, len(specs))
	relatedTypes := make([]*string, len(specs))
	for i, s := range specs {
		types[i] = s.JobType
		relatedIDs[i] = s.RelatedID
		relatedTypes[i] = s.RelatedType
	}

	_, err := db.Exec(ctx,
		`WITH inserted AS (
		     INSERT INTO jobs (type, related_id, related_type, parent_job_id)
		     SELECT t, r, rt, $4
		     FROM unnest($1::text[], $2::bigint[], $3::text[]) AS x(t, r, rt)
		     RETURNING 1
		 )
		 UPDATE jobs SET subjobs_enqueued = subjobs_enqueued + (SELECT count(*) FROM inserted)
		 WHERE id = $4`,
		types, relatedIDs, relatedTypes, parentID,
	)
	if err != nil {
		return 0, err
	}
	return len(specs), nil
}

// SubJobSpec holds the parameters for a single sub-job to be created in batch.
type SubJobSpec struct {
	JobType     string
	RelatedID   *int64
	RelatedType *string
}

// JobMarkRunning sets a job to running status with the current timestamp.
func JobMarkRunning(ctx context.Context, db utils.DBTX, jobID int64) error {
	_, err := db.Exec(ctx,
		`UPDATE jobs SET status = 'running', started_at = now() WHERE id = $1`,
		jobID,
	)
	return err
}

// JobMarkRunningSubJobs transitions a parent job to running_sub_jobs status.
func JobMarkRunningSubJobs(ctx context.Context, db utils.DBTX, jobID int64) error {
	_, err := db.Exec(ctx,
		`UPDATE jobs SET status = 'running_sub_jobs' WHERE id = $1`,
		jobID,
	)
	return err
}

// JobUpdateStep sets current_step for a job.
func JobUpdateStep(ctx context.Context, db utils.DBTX, jobID int64, step int) error {
	_, err := db.Exec(ctx,
		`UPDATE jobs SET current_step = $2 WHERE id = $1`,
		jobID, step,
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

// JobMarkDone sets a job to done status and removes its sub-jobs atomically.
func JobMarkDone(ctx context.Context, db utils.DBTX, jobID int64) error {
	_, err := db.Exec(ctx,
		`WITH cleanup AS (
		     DELETE FROM jobs WHERE parent_job_id = $1
		 )
		 UPDATE jobs SET status = 'done', completed_at = now() WHERE id = $1`,
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
		        queued_at, started_at, completed_at,
		        parent_job_id, current_step, subjobs_enqueued
		 FROM jobs WHERE id = $1`,
		jobID,
	).Scan(
		&j.ID, &j.Type, &j.Status, &j.RelatedID, &j.RelatedType,
		&j.Log, &j.ErrorMessage,
		&j.QueuedAt, &j.StartedAt, &j.CompletedAt,
		&j.ParentJobID, &j.CurrentStep, &j.SubjobsEnqueued,
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
		 WHERE type = $1 AND related_id = $2 AND status = ANY($3)
		   AND parent_job_id IS NULL`,
		jobType, relatedID, nonTerminalStatuses,
	).Scan(&count)
	return count > 0, err
}


// JobIsActiveByType returns true if any top-level job of the given type is queued or running.
func JobIsActiveByType(ctx context.Context, db utils.DBTX, jobType string) (bool, error) {
	var count int
	err := db.QueryRow(ctx,
		`SELECT COUNT(*) FROM jobs
		 WHERE type = $1 AND status = ANY($2)
		   AND parent_job_id IS NULL`,
		jobType, nonTerminalStatuses,
	).Scan(&count)
	return count > 0, err
}

// JobIsActiveForCollection checks if collection_scan is active for a collection.
func JobIsActiveForCollection(ctx context.Context, db utils.DBTX, collectionID int64) (bool, error) {
	var count int
	err := db.QueryRow(ctx,
		`SELECT COUNT(*) FROM jobs
		 WHERE type = 'collection_scan' AND related_id = $1
		   AND status = ANY($2)
		   AND parent_job_id IS NULL`,
		collectionID, nonTerminalStatuses,
	).Scan(&count)
	return count > 0, err
}

// JobsResetStuck resets any jobs left in 'running' state back to 'queued'.
// Skips jobs in 'running_sub_jobs' state (those are resumable).
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

// JobClaimSubJobBatch claims up to limit queued sub-jobs for a parent.
func JobClaimSubJobBatch(ctx context.Context, db utils.DBTX, parentID int64, limit int) ([]*models.Job, error) {
	rows, err := db.Query(ctx,
		`UPDATE jobs SET status = 'running', started_at = now()
		 WHERE id IN (
		     SELECT id FROM jobs
		     WHERE parent_job_id = $1 AND status = 'queued'
		     ORDER BY id
		     LIMIT $2
		     FOR UPDATE SKIP LOCKED
		 )
		 RETURNING queued_at, related_id, related_type, parent_job_id,
		           log, error_message,
		           started_at, completed_at,
		           type, status, id, current_step, subjobs_enqueued`,
		parentID, limit,
	)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToAddrOfStructByPos[models.Job])
}

// JobSubJobCounts returns (completed, total) sub-job counts for a parent.
func JobSubJobCounts(ctx context.Context, db utils.DBTX, parentID int64) (completed, total int64, err error) {
	err = db.QueryRow(ctx,
		`SELECT
		    COUNT(*) FILTER (WHERE status = 'done') AS completed,
		    COUNT(*) AS total
		 FROM jobs WHERE parent_job_id = $1`,
		parentID,
	).Scan(&completed, &total)
	return
}

// JobListTopLevel returns paginated top-level jobs (parent_job_id IS NULL), newest first.
func JobListTopLevel(ctx context.Context, db utils.DBTX, page, limit int, includeInactive bool) ([]*models.Job, int64, error) {
	offset := (page - 1) * limit

	// Determine how many total jobs there are
	var total int64
	if includeInactive {
		err := db.QueryRow(ctx,
			`SELECT COUNT(*) FROM jobs WHERE parent_job_id IS NULL`,
		).Scan(&total)
		if err != nil {
			return nil, 0, err
		}
	} else {
		err := db.QueryRow(ctx,
			`SELECT COUNT(*) FROM jobs WHERE parent_job_id IS NULL AND status = ANY($1)`,
			nonTerminalStatuses,
		).Scan(&total)
		if err != nil {
			return nil, 0, err
		}
	}

	const selectCols = `queued_at, related_id, related_type, parent_job_id,
		        log, error_message,
		        started_at, completed_at,
		        type, status, id, current_step, subjobs_enqueued`

	var rows pgx.Rows
	var err error
	if includeInactive {
		rows, err = db.Query(ctx,
			`SELECT `+selectCols+`
			 FROM jobs
			 WHERE parent_job_id IS NULL
			 ORDER BY queued_at DESC
			 LIMIT $1 OFFSET $2`,
			limit, offset,
		)
	} else {
		rows, err = db.Query(ctx,
			`SELECT `+selectCols+`
			 FROM jobs
			 WHERE parent_job_id IS NULL AND status = ANY($1)
			 ORDER BY queued_at DESC
			 LIMIT $2 OFFSET $3`,
			nonTerminalStatuses, limit, offset,
		)
	}
	if err != nil {
		return nil, 0, err
	}
	jobs, err := pgx.CollectRows(rows, pgx.RowToAddrOfStructByPos[models.Job])
	return jobs, total, err
}

// JobGetTranscodeParent finds an existing queued/running_sub_jobs video_transcode parent job.
func JobGetTranscodeParent(ctx context.Context, db utils.DBTX) (int64, error) {
	var id int64
	err := db.QueryRow(ctx,
		`SELECT id FROM jobs
		 WHERE type = 'video_transcode' AND status = ANY($1)
		   AND parent_job_id IS NULL
		 ORDER BY id ASC LIMIT 1`,
		nonTerminalStatuses,
	).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, pgx.ErrNoRows
	}
	return id, err
}
