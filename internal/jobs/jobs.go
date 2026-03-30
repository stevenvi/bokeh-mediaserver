package jobs

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/stevenvi/bokeh-mediaserver/internal/models"
	"github.com/stevenvi/bokeh-mediaserver/internal/repository"
	"github.com/stevenvi/bokeh-mediaserver/internal/utils"
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
	work        chan func()
	wg          sync.WaitGroup
	once        sync.Once
	closeCh     chan struct{}
	workerCount int
	activeCount atomic.Int32
}

// NewPool starts a pool with the given number of concurrent workers.
// Workers run until Close is called.
func NewPool(workers int) *Pool {
	p := &Pool{
		// Buffered to reduce lock contention on bursts, but the real
		// unbounded capacity comes from the goroutine below that feeds it.
		work:        make(chan func(), workers*4),
		closeCh:     make(chan struct{}),
		workerCount: workers,
	}

	for range workers {
		go func() {
			for {
				select {
				case fn, ok := <-p.work:
					if !ok {
						return
					}
					p.activeCount.Add(1)
					func() {
						defer p.wg.Done()
						defer p.activeCount.Add(-1)
						fn()
					}()
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

// IdleWorkers returns the number of workers not currently executing a task.
func (p *Pool) IdleWorkers() int {
	return p.workerCount - int(p.activeCount.Load())
}

// Workers returns the configured number of workers in this pool.
func (p *Pool) Workers() int {
	return p.workerCount
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
