package indexer

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/stevenvi/bokeh-mediaserver/internal/jobs"
	"github.com/stevenvi/bokeh-mediaserver/internal/models"
	"github.com/stevenvi/bokeh-mediaserver/internal/utils"
)

// processingWorker holds per-worker state for the processing pool.
// Each goroutine in the processing pool gets its own instance, which
// manages a persistent exiftool process to avoid per-file Perl startup.
type processingWorker struct {
	et     *utils.ExiftoolProcess
	mu     sync.Mutex
	closed bool
}

// exiftool returns the worker's exiftool process, creating it lazily on first use.
func (w *processingWorker) exiftool() (*utils.ExiftoolProcess, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.et == nil {
		var err error
		w.et, err = utils.NewExiftoolProcess()
		if err != nil {
			return nil, err
		}
	}
	return w.et, nil
}

// close shuts down the worker's exiftool process if one was started.
func (w *processingWorker) close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.et != nil && !w.closed {
		w.et.Close()
		w.closed = true
	}
}

// ProcessingWorkers manages a pool of processingWorker instances,
// one per processing pool goroutine. Jobs are round-robined across workers.
type ProcessingWorkers struct {
	workers []*processingWorker
	next    atomic.Int64
}

// NewProcessingWorkers creates N processing worker instances.
func NewProcessingWorkers(count int) *ProcessingWorkers {
	pw := &ProcessingWorkers{
		workers: make([]*processingWorker, count),
	}
	for i := range count {
		pw.workers[i] = &processingWorker{}
	}
	return pw
}

// CloseAll shuts down all exiftool processes.
func (pw *ProcessingWorkers) CloseAll() {
	for _, w := range pw.workers {
		w.close()
	}
}

// get returns the next worker in round-robin order.
func (pw *ProcessingWorkers) get() *processingWorker {
	idx := pw.next.Add(1) - 1
	return pw.workers[idx%int64(len(pw.workers))]
}

// HandleProcessMediaWithWorkers returns a JobHandler that routes process_media
// jobs through the shared ProcessingWorkers pool for exiftool reuse.
func HandleProcessMediaWithWorkers(pw *ProcessingWorkers, mediaPath, dataPath string) jobs.JobHandler {
	return func(ctx context.Context, db utils.DBTX, job *models.Job) error {
		worker := pw.get()
		handler := HandleProcessMedia(worker, mediaPath, dataPath)
		return handler(ctx, db, job)
	}
}
