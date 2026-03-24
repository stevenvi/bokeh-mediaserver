package jobs

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/stevenvi/bokeh-mediaserver/internal/repository"
	"github.com/stevenvi/bokeh-mediaserver/internal/utils"
)

const scheduleReloadInterval = 1 * time.Hour // re-read from DB hourly

// scheduledJob defines a recurring job type and its schedule configuration.
// If trigger is nil, the default behavior is triggerByType using jobType.
type scheduledJob struct {
	configKey       string                                  // column name in server_config
	defaultSchedule string                                  // fallback cron expression
	jobType         string                                  // jobs.type value
	trigger         func(s *Scheduler, ctx context.Context) // nil = use triggerByType
}

var scheduledJobs = []scheduledJob{
	{"scan_schedule", "0 3 * * *", "library_scan", (*Scheduler).TriggerScans},
	{"integrity_schedule", "0 4 * * 0", "integrity_check", nil},
	{"device_cleanup_schedule", "0 2 1 * *", "device_cleanup", nil},
	{"cover_cycle_schedule", "0 5 * * 1", "cover_cycle", nil},
}

// Scheduler reads cron schedules from server_config and creates jobs at the
// appropriate times. Schedules are re-read from the DB periodically to pick
// up admin changes without requiring a restart.
type Scheduler struct {
	jobs        *repository.JobRepository
	collections *repository.CollectionRepository
	cancel      context.CancelFunc
	wg          sync.WaitGroup
}

// NewScheduler creates a new scheduler.
func NewScheduler(db utils.DBTX) *Scheduler {
	return &Scheduler{
		jobs:        repository.NewJobRepository(db),
		collections: repository.NewCollectionRepository(db),
	}
}

// Start begins the scheduler loop.
func (s *Scheduler) Start(ctx context.Context) {
	ctx, s.cancel = context.WithCancel(ctx)
	s.wg.Go(func() {
		s.run(ctx)
	})
	slog.Info("scheduler started")
}

// Stop cancels the scheduler and waits for it to exit.
func (s *Scheduler) Stop() {
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
	slog.Info("scheduler stopped")
}

// ScheduleConfig maps config keys to their resolved cron expressions.
type ScheduleConfig map[string]string

func (s *Scheduler) LoadSchedules(ctx context.Context) ScheduleConfig {
	cfg := make(ScheduleConfig, len(scheduledJobs))
	for _, sj := range scheduledJobs {
		cfg[sj.configKey] = sj.defaultSchedule
	}

	overrides, err := s.jobs.LoadSchedules(ctx)
	if err != nil {
		slog.Warn("failed to load schedules from DB, using defaults", "err", err)
		return cfg
	}

	for key, val := range overrides {
		if val != nil && strings.TrimSpace(*val) != "" {
			cfg[key] = *val
		}
	}
	return cfg
}

func (s *Scheduler) run(ctx context.Context) {
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)

	type activeSchedule struct {
		job      scheduledJob
		schedule cron.Schedule
	}

	var schedules []activeSchedule
	var lastScheduleLoad time.Time

	reloadSchedules := func() {
		cfg := s.LoadSchedules(ctx)
		lastScheduleLoad = time.Now()

		schedules = schedules[:0]
		logAttrs := make([]any, 0, len(scheduledJobs)*2)
		for _, sj := range scheduledJobs {
			expr := cfg[sj.configKey]
			parsed, err := parser.Parse(expr)
			if err != nil {
				slog.Error("invalid schedule, using default", "key", sj.configKey, "schedule", expr, "err", err)
				parsed, _ = parser.Parse(sj.defaultSchedule)
			}
			schedules = append(schedules, activeSchedule{job: sj, schedule: parsed})
			logAttrs = append(logAttrs, sj.configKey, expr)
		}
		slog.Info("schedules loaded", logAttrs...)
	}

	reloadSchedules()

	for {
		now := time.Now()

		if time.Since(lastScheduleLoad) > scheduleReloadInterval {
			reloadSchedules()
			now = time.Now()
		}

		// Find earliest next trigger across all jobs.
		type pending struct {
			activeSchedule
			nextTime time.Time
		}
		items := make([]pending, len(schedules))
		earliest := time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC)
		for i, as := range schedules {
			items[i] = pending{as, as.schedule.Next(now)}
			if items[i].nextTime.Before(earliest) {
				earliest = items[i].nextTime
			}
		}

		logAttrs := make([]any, 0, len(items)*2)
		for _, p := range items {
			logAttrs = append(logAttrs, "next_"+p.job.configKey, p.nextTime)
		}
		slog.Debug("scheduler waiting", logAttrs...)

		timer := time.NewTimer(time.Until(earliest))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case t := <-timer.C:
			const tolerance = 30 * time.Second
			for _, p := range items {
				if t.After(p.nextTime.Add(-tolerance)) && t.Before(p.nextTime.Add(tolerance)) {
					if p.job.trigger != nil {
						p.job.trigger(s, ctx)
					} else {
						s.triggerByType(ctx, p.job.jobType)
					}
				}
			}
		}
	}
}

func (s *Scheduler) TriggerScans(ctx context.Context) {
	slog.Info("scheduled scan triggered")

	collIDs, err := s.collections.ListTopLevelEnabled(ctx)
	if err != nil {
		slog.Error("query collections for scheduled scan", "err", err)
		return
	}

	for _, collID := range collIDs {
		active, err := s.jobs.IsActive(ctx, "library_scan", collID)
		if err != nil {
			slog.Warn("check active scan", "collection_id", collID, "err", err)
			continue
		}
		if active {
			slog.Info("skipping scheduled scan — already active", "collection_id", collID)
			continue
		}

		relatedType := "collection"
		jobID, err := s.jobs.Create(ctx, "library_scan", &collID, &relatedType)
		if err != nil {
			slog.Error("create scheduled scan job", "collection_id", collID, "err", err)
			continue
		}
		slog.Info("queued scheduled scan", "collection_id", collID, "job_id", jobID)
	}
}

// TriggerIntegrityCheck triggers a scheduled integrity check.
func (s *Scheduler) TriggerIntegrityCheck(ctx context.Context) {
	s.triggerByType(ctx, "integrity_check")
}

// TriggerDeviceCleanup triggers a scheduled device cleanup.
func (s *Scheduler) TriggerDeviceCleanup(ctx context.Context) {
	s.triggerByType(ctx, "device_cleanup")
}

// TriggerCoverCycle triggers a scheduled cover cycle.
func (s *Scheduler) TriggerCoverCycle(ctx context.Context) {
	s.triggerByType(ctx, "cover_cycle")
}

func (s *Scheduler) triggerByType(ctx context.Context, jobType string) {
	slog.Info("scheduled job triggered", "type", jobType)

	active, err := s.jobs.IsActiveByType(ctx, jobType)
	if err != nil {
		slog.Error("check active job", "type", jobType, "err", err)
		return
	}
	if active {
		slog.Info("skipping scheduled job — already active", "type", jobType)
		return
	}

	jobID, err := s.jobs.Create(ctx, jobType, nil, nil)
	if err != nil {
		slog.Error("create scheduled job", "type", jobType, "err", err)
		return
	}
	slog.Info("queued scheduled job", "type", jobType, "job_id", jobID)
}
