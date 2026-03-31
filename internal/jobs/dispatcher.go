package jobs

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/stevenvi/bokeh-mediaserver/internal/models"
	"github.com/stevenvi/bokeh-mediaserver/internal/repository"
	"github.com/stevenvi/bokeh-mediaserver/internal/utils"
)

// JobHandler processes a single job. The job is already marked as 'running' in the DB.
// Return nil on success (job marked done) or an error (job marked failed).
type JobHandler func(ctx context.Context, db utils.DBTX, job *models.Job) error

type handlerEntry struct {
	handler     JobHandler
	lowPriority bool
}

// Dispatcher polls the database for queued jobs and routes them to the
// appropriate worker pool based on their registered handler.
type Dispatcher struct {
	db             utils.DBTX
	handlers       map[string]handlerEntry
	mainPool       *Pool
	processingPool *Pool
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	mu             sync.RWMutex
	paused         atomic.Bool
}

// Pause prevents new jobs from being claimed on the next poll cycle.
// In-flight jobs may continue running. Used when on-the-fly streaming starts.
func (d *Dispatcher) Pause() { d.paused.Store(true) }

// Resume allows the dispatcher to claim jobs again.
func (d *Dispatcher) Resume() { d.paused.Store(false) }

// NewDispatcher creates a dispatcher with the given worker pools.
// mainPool handles scan/maintenance jobs; processingPool handles media processing.
func NewDispatcher(db utils.DBTX, mainPool, processingPool *Pool) *Dispatcher {
	return &Dispatcher{
		db:             db,
		handlers:       make(map[string]handlerEntry),
		mainPool:       mainPool,
		processingPool: processingPool,
	}
}

// Register adds a handler for a job type. If lowPriority is true,
// the job is routed to the processing pool instead of the main pool.
func (d *Dispatcher) Register(jobType string, handler JobHandler, lowPriority bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.handlers[jobType] = handlerEntry{handler: handler, lowPriority: lowPriority}
}

// Start begins the polling loop in a background goroutine.
func (d *Dispatcher) Start(ctx context.Context) {
	ctx, d.cancel = context.WithCancel(ctx)
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		d.poll(ctx)
	}()
	slog.Info("job dispatcher started")
}

// Stop cancels the polling loop and waits for in-flight jobs to finish.
func (d *Dispatcher) Stop() {
	if d.cancel != nil {
		d.cancel()
	}
	d.wg.Wait()
	slog.Info("job dispatcher stopped")
}

func (d *Dispatcher) poll(ctx context.Context) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	// Do an immediate poll on startup to pick up recovered jobs
	d.pollOnce(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.pollOnce(ctx)
		}
	}
}

func (d *Dispatcher) pollOnce(ctx context.Context) {
	if d.paused.Load() {
		return
	}

	d.mu.RLock()
	defer d.mu.RUnlock()

	// Build lists of job types per pool
	var mainTypes, processingTypes []string
	for jobType, entry := range d.handlers {
		if entry.lowPriority {
			processingTypes = append(processingTypes, jobType)
		} else {
			mainTypes = append(mainTypes, jobType)
		}
	}

	// Claim jobs for main pool
	if len(mainTypes) > 0 {
		d.claimAndDispatch(ctx, mainTypes, d.mainPool)
	}

	// Claim jobs for processing pool
	if len(processingTypes) > 0 {
		d.claimAndDispatch(ctx, processingTypes, d.processingPool)
	}
}

func (d *Dispatcher) claimAndDispatch(ctx context.Context, jobTypes []string, pool *Pool) {
	// Keep claiming jobs while there are workers available
	for {
		if ctx.Err() != nil {
			return
		}

		job, err := repository.JobClaimNext(ctx, d.db, jobTypes)
		if err != nil {
			slog.Error("claim next job", "err", err)
			return
		}
		if job == nil {
			return // no more queued jobs of these types
		}

		entry, ok := d.handlers[job.Type]
		if !ok {
			slog.Error("no handler registered for job type", "type", job.Type)
			_ = repository.JobMarkFailed(ctx, d.db, job.ID, "no handler registered for job type: "+job.Type)
			continue
		}

		slog.Info("dispatching job", "job_id", job.ID, "type", job.Type)

		capturedJob := job
		capturedHandler := entry.handler
		pool.Submit(func() {
			if err := capturedHandler(ctx, d.db, capturedJob); err != nil {
				slog.Error("job failed", "job_id", capturedJob.ID, "type", capturedJob.Type, "err", err)
				_ = repository.JobMarkFailed(ctx, d.db, capturedJob.ID, err.Error())
			} else {
				_ = repository.JobMarkDone(ctx, d.db, capturedJob.ID)
			}
		})
	}
}
