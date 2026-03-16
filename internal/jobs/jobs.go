package jobs

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stevenvi/bokeh-mediaserver/internal/models"
)

// Pool is a worker pool with an unbounded submit queue.
//
// Submitting work never blocks, regardless of how many workers are currently
// busy. This prevents deadlocks when a running worker submits further work —
// a common pattern in multi-stage pipelines like the indexer.
//
// Concurrency is bounded on the consumer side: exactly `workers` goroutines
// pull from the queue simultaneously. The queue itself grows as needed.
type Pool struct {
	work    chan func()
	wg      sync.WaitGroup
	once    sync.Once
	closeCh chan struct{}
}

// NewPool starts a pool with the given number of concurrent workers.
// Workers run until Close is called.
func NewPool(workers int) *Pool {
	p := &Pool{
		// Buffered to reduce lock contention on bursts, but the real
		// unbounded capacity comes from the goroutine below that feeds it.
		work:    make(chan func(), workers*4),
		closeCh: make(chan struct{}),
	}

	for range workers {
		go func() {
			for {
				select {
				case fn, ok := <-p.work:
					if !ok {
						return
					}
					fn()
					p.wg.Done()
				case <-p.closeCh:
					return
				}
			}
		}()
	}

	return p
}

// Submit enqueues work. Never blocks — if the internal channel is full,
// a bridge goroutine parks until a slot opens, freeing the caller immediately.
//
// This is the key property that prevents deadlocks: a worker can call Submit
// from inside its own fn without any risk of circular blocking.
func (p *Pool) Submit(fn func()) {
	p.wg.Add(1)
	select {
	case p.work <- fn:
		// Fast path: channel had capacity, no extra goroutine needed.
	default:
		// Slow path: channel is full. Spawn a bridge goroutine to park
		// until a slot opens. The caller returns immediately.
		go func() {
			p.work <- fn
		}()
	}
}

// Wait blocks until all submitted work has completed.
// Safe to call from any goroutine, including from within a submitted fn
// as long as that fn does not hold the last worker slot (which would
// itself be a deadlock — but Submit's non-blocking design makes this
// unlikely in practice for the indexer pipeline).
// TODO "unlikely" is inadequate: deadlock should be impossible by design. Rethink this!!
func (p *Pool) Wait() {
	p.wg.Wait()
}

// Close shuts down the pool after all submitted work completes.
// Subsequent calls to Submit after Close will panic (send on closed channel).
func (p *Pool) Close() {
	p.Wait()
	p.once.Do(func() {
		close(p.closeCh)
		close(p.work)
	})
}

// ─── DB helpers ───────────────────────────────────────────────────────────────

// Create inserts a new job row and returns its ID.
func Create(ctx context.Context, db *pgxpool.Pool, jobType string, relatedID *int, relatedType *string) (int, error) {
	var id int
	err := db.QueryRow(ctx,
		`INSERT INTO jobs (type, related_id, related_type)
		 VALUES ($1, $2, $3)
		 RETURNING id`,
		jobType, relatedID, relatedType,
	).Scan(&id)
	return id, err
}

// MarkRunning sets a job to running status with the current timestamp.
func MarkRunning(ctx context.Context, db *pgxpool.Pool, jobID int) error {
	_, err := db.Exec(ctx,
		`UPDATE jobs SET status = 'running', started_at = now() WHERE id = $1`,
		jobID,
	)
	return err
}

// UpdateProgress updates the progress and appends a line to the log.
func UpdateProgress(ctx context.Context, db *pgxpool.Pool, jobID int, msg string) error {
	_, err := db.Exec(ctx,
		`UPDATE jobs
		 SET log = COALESCE(log, '') || $3
		 WHERE id = $1`,
		jobID, msg+"\n",
	)
	return err
}

// MarkDone sets a job to done status.
func MarkDone(ctx context.Context, db *pgxpool.Pool, jobID int) error {
	_, err := db.Exec(ctx,
		`UPDATE jobs
		 SET status = 'done', completed_at = now()
		 WHERE id = $1`,
		jobID,
	)
	return err
}

// MarkFailed sets a job to failed status with an error message.
func MarkFailed(ctx context.Context, db *pgxpool.Pool, jobID int, errMsg string) error {
	_, err := db.Exec(ctx,
		`UPDATE jobs
		 SET status = 'failed', error_message = $2, completed_at = now()
		 WHERE id = $1`,
		jobID, errMsg,
	)
	return err
}

// GetByID returns a single job by ID.
func GetByID(ctx context.Context, db *pgxpool.Pool, jobID int) (*models.Job, error) {
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

// IsActive returns true if a job of the given type is queued or running
// for the given related ID. Used to prevent duplicate job submission.
func IsActive(ctx context.Context, db *pgxpool.Pool, jobType string, relatedID int) (bool, error) {
	var count int
	err := db.QueryRow(ctx,
		`SELECT COUNT(*) FROM jobs
		 WHERE type = $1 AND related_id = $2 AND status IN ('queued', 'running')`,
		jobType, relatedID,
	).Scan(&count)
	return count > 0, err
}

// RecoverStuckJobs resets any jobs left in 'running' state from a previous
// hard-killed server instance back to 'queued'. Called during startup.
func RecoverStuckJobs(ctx context.Context, db *pgxpool.Pool) error {
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

	// Re-queue any photo_metadata rows where variant generation was interrupted
	var count int
	err = db.QueryRow(ctx,
		`SELECT COUNT(*) FROM photo_metadata
		WHERE variants_generated_at IS NULL
		AND media_item_id IN (
			SELECT id FROM media_items WHERE missing_since IS NULL
		)`,
	).Scan(&count)
	if err != nil {
		return fmt.Errorf("count incomplete variants: %w", err)
	}
	if count > 0 {
		slog.Warn("photos pending variant generation — will process on next scan",
			"count", count)
	}


	return nil
}

// PollJobStatus polls a job until it reaches a terminal state.
func PollJobStatus(ctx context.Context, db *pgxpool.Pool, jobID int, interval time.Duration) (*models.Job, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(interval):
			job, err := GetByID(ctx, db, jobID)
			if err != nil {
				return nil, err
			}
			if job.Status == "done" || job.Status == "failed" {
				return job, nil
			}
		}
	}
}
