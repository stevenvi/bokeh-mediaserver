package jobs

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/robfig/cron/v3"
	"github.com/stevenvi/bokeh-mediaserver/internal/repository"
	"github.com/stevenvi/bokeh-mediaserver/internal/utils"
)

// Scheduler reads job schedules from the jobs_schedule table and enqueues jobs
// at the appropriate times. It supports live reload via NotifyReload().
type Scheduler struct {
	db         utils.DBTX
	dispatcher *Dispatcher
	reloadCh   chan struct{}
	cancel     context.CancelFunc
	wg         sync.WaitGroup
}

// NewScheduler creates a new scheduler.
func NewScheduler(db *pgxpool.Pool, dispatcher *Dispatcher) *Scheduler {
	return &Scheduler{
		db:         db,
		dispatcher: dispatcher,
		reloadCh:   make(chan struct{}, 1),
	}
}

// NotifyReload signals the scheduler to re-read the jobs_schedule table
// and rebuild its cron schedules.
func (s *Scheduler) NotifyReload() {
	select {
	case s.reloadCh <- struct{}{}:
	default:
	}
}

// Start begins the scheduler loop.
func (s *Scheduler) Start(ctx context.Context) {
	ctx, s.cancel = context.WithCancel(ctx)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.run(ctx)
	}()
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

func (s *Scheduler) run(ctx context.Context) {
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)

	type activeSchedule struct {
		name     string
		schedule cron.Schedule
	}

	var schedules []activeSchedule
	var lastLoad time.Time

	reloadSchedules := func() {
		rows, err := repository.JobScheduleList(ctx, s.db)
		if err != nil {
			slog.Warn("failed to load job schedules", "err", err)
			return
		}
		lastLoad = time.Now()

		schedules = schedules[:0]
		logAttrs := make([]any, 0, len(rows)*2)
		for _, row := range rows {
			parsed, err := parser.Parse(row.Cron)
			if err != nil {
				slog.Warn("invalid schedule cron expression", "name", row.Name, "cron", row.Cron, "err", err)
				continue
			}
			schedules = append(schedules, activeSchedule{name: row.Name, schedule: parsed})
			logAttrs = append(logAttrs, row.Name, row.Cron)
		}
		slog.Info("schedules loaded", logAttrs...)
	}

	reloadSchedules()

	for {
		if ctx.Err() != nil {
			return
		}

		now := time.Now()

		// Find earliest next trigger across all schedules.
		type pending struct {
			activeSchedule
			nextTime time.Time
		}

		if len(schedules) == 0 {
			// No schedules — wait for reload or context cancellation.
			select {
			case <-ctx.Done():
				return
			case <-s.reloadCh:
				reloadSchedules()
			case <-time.After(1 * time.Hour):
				reloadSchedules()
			}
			continue
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
			logAttrs = append(logAttrs, "next_"+p.name, p.nextTime)
		}
		slog.Debug("scheduler waiting", logAttrs...)

		timer := time.NewTimer(time.Until(earliest))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-s.reloadCh:
			timer.Stop()
			reloadSchedules()
		case <-time.After(1 * time.Hour):
			// Periodic reload to pick up any DB changes not triggered via NotifyReload.
			if time.Since(lastLoad) > time.Hour {
				reloadSchedules()
			}
		case t := <-timer.C:
			const tolerance = 30 * time.Second
			for _, p := range items {
				if t.After(p.nextTime.Add(-tolerance)) && t.Before(p.nextTime.Add(tolerance)) {
					s.triggerSchedule(ctx, p.name)
				}
			}
		}
	}
}

// triggerSchedule fires the appropriate action for a scheduled job name.
func (s *Scheduler) triggerSchedule(ctx context.Context, name string) {
	slog.Info("scheduled trigger", "name", name)

	switch name {
	case "collection_scan":
		s.triggerCollectionScans(ctx)
	default:
		s.triggerByType(ctx, name)
	}
}

// triggerCollectionScans enqueues a collection_scan for each enabled top-level collection
// that doesn't already have an active scan.
// TODO: This should probably not be handled here...
func (s *Scheduler) triggerCollectionScans(ctx context.Context) {
	collIDs, err := repository.CollectionsTopLevelEnabled(ctx, s.db)
	if err != nil {
		slog.Error("query collections for scheduled scan", "err", err)
		return
	}

	for _, collID := range collIDs {
		active, err := repository.JobIsActiveForCollection(ctx, s.db, collID)
		if err != nil {
			slog.Warn("check active collection scan", "collection_id", collID, "err", err)
			continue
		}
		if active {
			slog.Info("skipping scheduled collection scan — already active", "collection_id", collID)
			continue
		}

		relatedType := "collection"
		if _, err := s.dispatcher.Enqueue(ctx, "collection_scan", &collID, &relatedType); err != nil {
			slog.Error("enqueue scheduled collection scan", "collection_id", collID, "err", err)
			continue
		}
		slog.Info("queued scheduled collection scan", "collection_id", collID)
	}
}

// triggerByType enqueues a simple (no related entity) job if not already active.
func (s *Scheduler) triggerByType(ctx context.Context, jobType string) {
	active, err := repository.JobIsActiveByType(ctx, s.db, jobType)
	if err != nil {
		slog.Error("check active job", "type", jobType, "err", err)
		return
	}
	if active {
		slog.Info("skipping scheduled job — already active", "type", jobType)
		return
	}

	if _, err := s.dispatcher.Enqueue(ctx, jobType, nil, nil); err != nil {
		slog.Error("enqueue scheduled job", "type", jobType, "err", err)
		return
	}
	slog.Info("queued scheduled job", "type", jobType)
}
