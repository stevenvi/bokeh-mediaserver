package jobs

import (
	"context"
	"errors"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	jobsutils "github.com/stevenvi/bokeh-mediaserver/internal/jobs/utils"
	"github.com/stevenvi/bokeh-mediaserver/internal/models"
	"github.com/stevenvi/bokeh-mediaserver/internal/repository"
	"github.com/stevenvi/bokeh-mediaserver/internal/utils"
)

type registeredHandler struct {
	meta    JobMeta
	handler JobHandler
}

type queuedJob struct {
	id      int64
	jobType string
}

// Dispatcher runs jobs sequentially (one top-level at a time) with parallel sub-job execution.
type Dispatcher struct {
	db       utils.DBTX
	handlers map[string]registeredHandler
	mu       sync.RWMutex

	queue   []queuedJob
	queueMu sync.Mutex
	wakeCh  chan struct{}

	ctx    context.Context
	cancel context.CancelFunc
	doneCh chan struct{}

	et   *jobsutils.ExiftoolProcess
	etMu sync.Mutex

	paused atomic.Bool
}

// NewDispatcher creates a new sequential dispatcher.
func NewDispatcher(db utils.DBTX) *Dispatcher {
	return &Dispatcher{
		db:       db,
		handlers: make(map[string]registeredHandler),
		wakeCh:   make(chan struct{}, 1),
		doneCh:   make(chan struct{}),
	}
}

// Register adds a handler for a job type with its metadata.
func (d *Dispatcher) Register(jobType string, meta JobMeta, handler JobHandler) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.handlers[jobType] = registeredHandler{meta: meta, handler: handler}
}

// GetMeta returns the registered meta for a job type.
func (d *Dispatcher) GetMeta(jobType string) (JobMeta, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	h, ok := d.handlers[jobType]
	return h.meta, ok
}

// Pause prevents new jobs from being started.
func (d *Dispatcher) Pause() { d.paused.Store(true) }

// Resume allows the dispatcher to start jobs again.
func (d *Dispatcher) Resume() { d.paused.Store(false) }

// TriggerImmediately wakes the run loop. Safe to call from any goroutine.
func (d *Dispatcher) TriggerImmediately() {
	select {
	case d.wakeCh <- struct{}{}:
	default:
	}
}

// Enqueue creates a job in DB, adds to in-memory queue, and signals the run loop.
func (d *Dispatcher) Enqueue(ctx context.Context, jobType string, relatedID *int64, relatedType *string) (int64, error) {
	id, err := repository.JobCreate(ctx, d.db, jobType, relatedID, relatedType, nil)
	if err != nil {
		return 0, err
	}
	d.queueMu.Lock()
	d.queue = append(d.queue, queuedJob{id: id, jobType: jobType})
	d.queueMu.Unlock()
	d.TriggerImmediately()
	return id, nil
}

// findOrCreateTranscodeParent finds or creates a video_transcode parent job.
func (d *Dispatcher) findOrCreateTranscodeParent(ctx context.Context, db utils.DBTX) (int64, error) {
	parentID, err := repository.JobGetTranscodeParent(ctx, db)
	if err == nil {
		return parentID, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return 0, err
	}
	// Create a new video_transcode parent
	id, err := repository.JobCreate(ctx, d.db, "video_transcode", nil, nil, nil)
	if err != nil {
		return 0, err
	}
	// Add to in-memory queue
	d.queueMu.Lock()
	d.queue = append(d.queue, queuedJob{id: id, jobType: "video_transcode"})
	d.queueMu.Unlock()
	return id, nil
}

// Start begins the run loop in a background goroutine.
func (d *Dispatcher) Start(ctx context.Context) {
	ctx, d.cancel = context.WithCancel(ctx)
	d.ctx = ctx
	go func() {
		defer close(d.doneCh)
		d.run(ctx)
	}()
	slog.Info("job dispatcher started")
}

// Stop cancels the run loop and waits for it to exit.
func (d *Dispatcher) Stop() {
	if d.cancel != nil {
		d.cancel()
	}
	<-d.doneCh
	slog.Info("job dispatcher stopped")
}

func (d *Dispatcher) run(ctx context.Context) {
	pollTimer := time.NewTicker(15 * time.Second)
	defer pollTimer.Stop()

	// Initial poll to recover running_sub_jobs and queued jobs
	d.loadFromDB(ctx)
	d.processNext(ctx)

	for {
		select {
		case <-ctx.Done():
			d.closeExiftool()
			return
		case <-d.wakeCh:
			d.processNext(ctx)
		case <-pollTimer.C:
			d.loadFromDB(ctx)
			d.processNext(ctx)
		}
	}
}

// loadFromDB loads any queued/running_sub_jobs top-level jobs from DB into the in-memory queue.
func (d *Dispatcher) loadFromDB(ctx context.Context) {
	rows, err := d.db.Query(ctx,
		`SELECT id, type FROM jobs
		 WHERE status IN ('queued', 'running_sub_jobs') AND parent_job_id IS NULL
		 ORDER BY queued_at ASC`,
	)
	if err != nil {
		slog.Warn("load jobs from DB", "err", err)
		return
	}
	defer rows.Close()

	d.queueMu.Lock()
	defer d.queueMu.Unlock()

	// Build set of existing IDs to avoid duplicates
	existing := make(map[int64]struct{}, len(d.queue))
	for _, q := range d.queue {
		existing[q.id] = struct{}{}
	}

	for rows.Next() {
		var id int64
		var jobType string
		if err := rows.Scan(&id, &jobType); err != nil {
			continue
		}
		if _, ok := existing[id]; !ok {
			d.queue = append(d.queue, queuedJob{id: id, jobType: jobType})
		}
	}
}

// processNext picks and executes the next job from the queue.
func (d *Dispatcher) processNext(ctx context.Context) {
	if d.paused.Load() {
		return
	}

	job := d.pickNext()
	if job == nil {
		d.closeExiftool()
		return
	}

	// Fetch job from DB
	dbJob, err := repository.JobGet(ctx, d.db, job.id)
	if err != nil {
		slog.Warn("fetch job for execution", "job_id", job.id, "err", err)
		return
	}

	// If it's already in running_sub_jobs state, resume sub-job processing
	if dbJob.Status == "running_sub_jobs" {
		d.mu.RLock()
		entry, ok := d.handlers[dbJob.Type]
		d.mu.RUnlock()
		if ok {
			d.executeSubJobs(ctx, dbJob, entry.meta)
		}
		return
	}

	if dbJob.Status != "queued" {
		return
	}

	d.mu.RLock()
	entry, ok := d.handlers[dbJob.Type]
	d.mu.RUnlock()
	if !ok {
		slog.Error("no handler for job type", "type", dbJob.Type, "job_id", dbJob.ID)
		_ = repository.JobMarkFailed(ctx, d.db, dbJob.ID, "no handler registered for job type: "+dbJob.Type)
		return
	}

	// Mark running
	if err := repository.JobMarkRunning(ctx, d.db, dbJob.ID); err != nil {
		slog.Warn("mark job running", "job_id", dbJob.ID, "err", err)
		return
	}
	dbJob.Status = "running"

	slog.Info("dispatching job", "job_id", dbJob.ID, "type", dbJob.Type)

	// Ensure exiftool is available
	et, err := d.getExiftool()
	if err != nil {
		slog.Error("start exiftool", "err", err)
		_ = repository.JobMarkFailed(ctx, d.db, dbJob.ID, "exiftool init: "+err.Error())
		return
	}

	jc := &JobContext{
		DB:         d.db,
		Job:        dbJob,
		Et:         et,
		dispatcher: d,
	}

	// Execute handler
	handlerErr := entry.handler(ctx, jc)

	if handlerErr != nil {
		slog.Error("job failed", "job_id", dbJob.ID, "type", dbJob.Type, "err", handlerErr)
		_ = repository.JobMarkFailed(ctx, d.db, dbJob.ID, handlerErr.Error())
		return
	}

	// Flush sub-jobs
	count, err := jc.FlushSubJobs(ctx)
	if err != nil {
		slog.Warn("flush sub-jobs", "job_id", dbJob.ID, "err", err)
	}

	if count > 0 && entry.meta.SupportsSubjobs {
		slog.Info("job has sub-jobs, transitioning", "job_id", dbJob.ID, "sub_jobs", count)
		_ = repository.JobMarkRunningSubJobs(ctx, d.db, dbJob.ID)
		d.executeSubJobs(ctx, dbJob, entry.meta)
	} else {
		_ = repository.JobMarkDone(ctx, d.db, dbJob.ID)
		slog.Info("job complete", "job_id", dbJob.ID, "type", dbJob.Type)
	}
}

// pickNext returns the next job to process, giving priority to non-transcode jobs.
func (d *Dispatcher) pickNext() *queuedJob {
	d.queueMu.Lock()
	defer d.queueMu.Unlock()

	if len(d.queue) == 0 {
		return nil
	}

	// Check if there are non-transcode jobs (they have priority)
	for i, q := range d.queue {
		if q.jobType != "video_transcode" {
			job := d.queue[i]
			d.queue = append(d.queue[:i], d.queue[i+1:]...)
			return &job
		}
	}

	// Only transcode jobs remain
	job := d.queue[0]
	d.queue = d.queue[1:]
	return &job
}

// hasNonTranscodeJobs returns true if any queued job is not a transcode.
func (d *Dispatcher) hasNonTranscodeJobs() bool {
	d.queueMu.Lock()
	defer d.queueMu.Unlock()
	for _, q := range d.queue {
		if q.jobType != "video_transcode" {
			return true
		}
	}
	return false
}

// executeSubJobs runs all sub-jobs for the given parent job.
func (d *Dispatcher) executeSubJobs(ctx context.Context, parent *models.Job, meta JobMeta) {
	maxConcurrency := meta.MaxConcurrency
	if maxConcurrency <= 0 {
		maxConcurrency = runtime.NumCPU()
	}

	isTranscode := parent.Type == "video_transcode"

	for {
		if ctx.Err() != nil {
			return
		}

		// For transcode jobs: yield if higher-priority jobs are waiting
		if isTranscode && d.hasNonTranscodeJobs() {
			slog.Info("video_transcode yielding to higher-priority job", "parent_id", parent.ID)
			// Put back in queue
			d.queueMu.Lock()
			d.queue = append([]queuedJob{{id: parent.ID, jobType: parent.Type}}, d.queue...)
			d.queueMu.Unlock()
			d.TriggerImmediately()
			return
		}

		batch, err := repository.JobClaimSubJobBatch(ctx, d.db, parent.ID, 100)
		if err != nil {
			slog.Warn("claim sub-job batch", "parent_id", parent.ID, "err", err)
			break
		}
		if len(batch) == 0 {
			break
		}

		// Refresh exiftool each batch so a dead process from a prior batch is replaced.
		et, err := d.getExiftool()
		if err != nil {
			slog.Error("exiftool for sub-jobs", "err", err)
			et = nil
		}

		sem := make(chan struct{}, maxConcurrency)
		var wg sync.WaitGroup
		for _, subjob := range batch {
			sem <- struct{}{}
			wg.Add(1)
			go func(j *models.Job) {
				defer wg.Done()
				defer func() { <-sem }()
				d.runSubJob(ctx, j, et, parent)
			}(subjob)
		}
		wg.Wait()

		// After batch: check for new sub-jobs added (e.g. for video_transcode)
		if isTranscode {
			completed, total, _ := repository.JobSubJobCounts(ctx, d.db, parent.ID)
			slog.Debug("transcode sub-job batch complete", "parent_id", parent.ID, "completed", completed, "total", total)
		}
	}

	// Delete sub-job rows and mark parent done
	_ = repository.JobMarkDone(ctx, d.db, parent.ID)
	slog.Info("job complete (all sub-jobs done)", "job_id", parent.ID, "type", parent.Type)
}

// runSubJob executes a single sub-job.
func (d *Dispatcher) runSubJob(ctx context.Context, job *models.Job, et *jobsutils.ExiftoolProcess, parent *models.Job) {
	d.mu.RLock()
	entry, ok := d.handlers[job.Type]
	d.mu.RUnlock()
	if !ok {
		slog.Error("no handler for sub-job type", "type", job.Type, "job_id", job.ID)
		_ = repository.JobMarkFailed(ctx, d.db, job.ID, "no handler for type: "+job.Type)
		return
	}

	jc := &JobContext{
		DB:  d.db,
		Job: job,
		Et:  et,
	}

	if err := entry.handler(ctx, jc); err != nil {
		slog.Warn("sub-job failed", "job_id", job.ID, "type", job.Type, "parent_id", parent.ID, "err", err)
		_ = repository.JobMarkFailed(ctx, d.db, job.ID, err.Error())
	} else {
		_ = repository.JobMarkDone(ctx, d.db, job.ID)
	}
}

func (d *Dispatcher) getExiftool() (*jobsutils.ExiftoolProcess, error) {
	d.etMu.Lock()
	defer d.etMu.Unlock()
	if d.et != nil && d.et.IsDead() {
		d.et.Close()
		d.et = nil
	}
	if d.et == nil {
		var err error
		d.et, err = jobsutils.NewExiftoolProcess()
		if err != nil {
			return nil, err
		}
	}
	return d.et, nil
}

func (d *Dispatcher) closeExiftool() {
	d.etMu.Lock()
	defer d.etMu.Unlock()
	if d.et != nil {
		d.et.Close()
		d.et = nil
	}
}

// PollStatus polls a job until it reaches a terminal state (done or failed).
func PollStatus(ctx context.Context, db utils.DBTX, jobID int64, interval time.Duration) (*models.Job, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(interval):
			job, err := repository.JobGet(ctx, db, jobID)
			if err != nil {
				return nil, err
			}
			if job.Status == "done" || job.Status == "failed" {
				return job, nil
			}
		}
	}
}
