package jobs

import (
	"context"
	"log/slog"
	"sync"

	jobsutils "github.com/stevenvi/bokeh-mediaserver/internal/jobs/utils"
	"github.com/stevenvi/bokeh-mediaserver/internal/models"
	"github.com/stevenvi/bokeh-mediaserver/internal/repository"
	"github.com/stevenvi/bokeh-mediaserver/internal/utils"
)

// JobMeta describes a registered job type.
type JobMeta struct {
	Description     string
	TotalSteps      int
	SupportsSubjobs bool
	MaxConcurrency  int // 0 = runtime.NumCPU(), 1 = sequential
}

// subJobSpec holds the details of a sub-job to be created.
type subJobSpec struct {
	relatedID   *int64
	relatedType *string
	jobType     string
}

// JobContext is passed to every job handler.
type JobContext struct {
	DB         utils.DBTX
	Job        *models.Job
	Et         *jobsutils.ExiftoolProcess
	dispatcher *Dispatcher
	subJobBuf  []subJobSpec
	mu         sync.Mutex
}

// SetStep updates the job's current_step in the DB.
func (jc *JobContext) SetStep(ctx context.Context, n int) {
	jc.Job.CurrentStep = n
	if err := repository.JobUpdateStep(ctx, jc.DB, jc.Job.ID, n); err != nil {
		slog.Warn("update job step", "job_id", jc.Job.ID, "step", n, "err", err)
	}
}

// AddSubJob buffers a sub-job to be flushed to DB after the handler returns.
func (jc *JobContext) AddSubJob(jobType string, relatedID *int64, relatedType *string) {
	jc.mu.Lock()
	defer jc.mu.Unlock()
	jc.subJobBuf = append(jc.subJobBuf, subJobSpec{
		jobType:     jobType,
		relatedID:   relatedID,
		relatedType: relatedType,
	})
	if len(jc.subJobBuf)%1000 == 0 {
		slog.Info("buffered sub-jobs", "count", len(jc.subJobBuf), "job_id", jc.Job.ID)
	}
}

// AttachTranscodeSubJob finds or creates a queued video_transcode parent job
// and attaches a sub-job for the given media item.
func (jc *JobContext) AttachTranscodeSubJob(ctx context.Context, mediaItemID int64) {
	if jc.dispatcher == nil {
		return
	}
	relatedType := "media_item"
	parentID, err := jc.dispatcher.findOrCreateTranscodeParent(ctx, jc.DB)
	if err != nil {
		slog.Warn("find/create transcode parent", "err", err)
		return
	}
	if _, err := repository.JobCreate(ctx, jc.DB, "video_transcode_item", &mediaItemID, &relatedType, &parentID); err != nil {
		slog.Warn("create transcode sub-job", "media_item_id", mediaItemID, "err", err)
	}
}

// SubJobCount returns how many sub-jobs have been buffered.
func (jc *JobContext) SubJobCount() int {
	jc.mu.Lock()
	defer jc.mu.Unlock()
	return len(jc.subJobBuf)
}

// FlushSubJobs writes buffered sub-jobs to the DB in batches with parent_job_id set.
// Returns the number of sub-jobs created.
func (jc *JobContext) FlushSubJobs(ctx context.Context) (int, error) {
	jc.mu.Lock()
	buf := jc.subJobBuf
	jc.subJobBuf = nil
	jc.mu.Unlock()

	if len(buf) == 0 {
		return 0, nil
	}

	const batchSize = 500
	total := 0
	for i := 0; i < len(buf); i += batchSize {
		end := i + batchSize
		if end > len(buf) {
			end = len(buf)
		}
		chunk := buf[i:end]

		specs := make([]repository.SubJobSpec, len(chunk))
		for j, s := range chunk {
			specs[j] = repository.SubJobSpec{
				JobType:     s.jobType,
				RelatedID:   s.relatedID,
				RelatedType: s.relatedType,
			}
		}

		n, err := repository.JobCreateSubJobBatch(ctx, jc.DB, jc.Job.ID, specs)
		if err != nil {
			slog.Warn("flush sub-job batch", "offset", i, "count", len(chunk), "err", err)
			continue
		}
		total += n
	}
	return total, nil
}

// JobHandler processes a single job. The job is already marked as 'running' in the DB.
type JobHandler func(ctx context.Context, jc *JobContext) error
