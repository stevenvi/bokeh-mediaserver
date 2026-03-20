package jobs

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/stevenvi/bokeh-mediaserver/internal/repository"
	"github.com/stevenvi/bokeh-mediaserver/internal/utils"
)

const (
	defaultScanSchedule          = "0 3 * * *"   // 3 AM daily
	defaultIntegritySchedule     = "0 4 * * 0"   // 4 AM Sundays
	defaultDeviceCleanupSchedule = "0 2 1 * *"   // 2 AM on the 1st of each month
	scheduleReloadInterval       = 1 * time.Hour // re-read from DB hourly
)

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

type ScheduleConfig struct {
	ScanSchedule          string
	IntegritySchedule     string
	DeviceCleanupSchedule string
}

func (s *Scheduler) LoadSchedules(ctx context.Context) ScheduleConfig {
	scanSched, integritySched, deviceCleanupSched, err := s.jobs.LoadSchedules(ctx)
	if err != nil {
		slog.Warn("failed to load schedules from DB, using defaults", "err", err)
		return ScheduleConfig{
			ScanSchedule:          defaultScanSchedule,
			IntegritySchedule:     defaultIntegritySchedule,
			DeviceCleanupSchedule: defaultDeviceCleanupSchedule,
		}
	}

	cfg := ScheduleConfig{
		ScanSchedule:          defaultScanSchedule,
		IntegritySchedule:     defaultIntegritySchedule,
		DeviceCleanupSchedule: defaultDeviceCleanupSchedule,
	}
	// Note: validation is performed in run function
	if scanSched != nil && *scanSched != "" {
		cfg.ScanSchedule = *scanSched
	}
	if integritySched != nil && *integritySched != "" {
		cfg.IntegritySchedule = *integritySched
	}
	if deviceCleanupSched != nil && *deviceCleanupSched != "" {
		cfg.DeviceCleanupSchedule = *deviceCleanupSched
	}
	return cfg
}

func (s *Scheduler) run(ctx context.Context) {
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)

	var lastScheduleLoad time.Time
	var scanSched, integritySched, deviceCleanupSched cron.Schedule

	reloadSchedules := func() {
		cfg := s.LoadSchedules(ctx)
		lastScheduleLoad = time.Now()

		var err error
		scanSched, err = parser.Parse(cfg.ScanSchedule)
		if err != nil {
			slog.Error("invalid scan_schedule, using default", "schedule", cfg.ScanSchedule, "err", err)
			scanSched, _ = parser.Parse(defaultScanSchedule)
		}
		integritySched, err = parser.Parse(cfg.IntegritySchedule)
		if err != nil {
			slog.Error("invalid integrity_schedule, using default", "schedule", cfg.IntegritySchedule, "err", err)
			integritySched, _ = parser.Parse(defaultIntegritySchedule)
		}
		deviceCleanupSched, err = parser.Parse(cfg.DeviceCleanupSchedule)
		if err != nil {
			slog.Error("invalid device_cleanup_schedule, using default", "schedule", cfg.DeviceCleanupSchedule, "err", err)
			deviceCleanupSched, _ = parser.Parse(defaultDeviceCleanupSchedule)
		}

		slog.Info("schedules loaded",
			"scan", cfg.ScanSchedule,
			"integrity", cfg.IntegritySchedule,
			"device_cleanup", cfg.DeviceCleanupSchedule)
	}

	reloadSchedules()

	for {
		now := time.Now()

		// Periodically reload schedules from DB
		if time.Since(lastScheduleLoad) > scheduleReloadInterval {
			reloadSchedules()
			now = time.Now()
		}

		// Find next trigger time
		nextScan := scanSched.Next(now)
		nextIntegrity := integritySched.Next(now)
		nextDeviceCleanup := deviceCleanupSched.Next(now)

		next := nextScan
		if nextIntegrity.Before(next) {
			next = nextIntegrity
		}
		if nextDeviceCleanup.Before(next) {
			next = nextDeviceCleanup
		}

		slog.Debug("scheduler waiting",
			"next_scan", nextScan,
			"next_integrity", nextIntegrity,
			"next_device_cleanup", nextDeviceCleanup)

		timer := time.NewTimer(time.Until(next))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case t := <-timer.C:
			// Check which schedule(s) triggered
			// Use a small tolerance window (30s) for matching
			if t.After(nextScan.Add(-30*time.Second)) && t.Before(nextScan.Add(30*time.Second)) {
				s.TriggerScans(ctx)
			}
			if t.After(nextIntegrity.Add(-30*time.Second)) && t.Before(nextIntegrity.Add(30*time.Second)) {
				s.TriggerIntegrityCheck(ctx)
			}
			if t.After(nextDeviceCleanup.Add(-30*time.Second)) && t.Before(nextDeviceCleanup.Add(30*time.Second)) {
				s.TriggerDeviceCleanup(ctx)
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

func (s *Scheduler) TriggerIntegrityCheck(ctx context.Context) {
	slog.Info("scheduled integrity check triggered")

	// Only create if not already active
	active, err := s.jobs.IsActiveByType(ctx, "integrity_check")
	if err != nil {
		slog.Error("check active integrity job", "err", err)
		return
	}
	if active {
		slog.Info("skipping scheduled integrity check — already active")
		return
	}

	jobID, err := s.jobs.Create(ctx, "integrity_check", nil, nil)
	if err != nil {
		slog.Error("create scheduled integrity job", "err", err)
		return
	}
	slog.Info("queued scheduled integrity check", "job_id", jobID)
}

func (s *Scheduler) TriggerDeviceCleanup(ctx context.Context) {
	slog.Info("scheduled device cleanup triggered")

	active, err := s.jobs.IsActiveByType(ctx, "device_cleanup")
	if err != nil {
		slog.Error("check active device cleanup job", "err", err)
		return
	}
	if active {
		slog.Info("skipping scheduled device cleanup — already active")
		return
	}

	jobID, err := s.jobs.Create(ctx, "device_cleanup", nil, nil)
	if err != nil {
		slog.Error("create scheduled device cleanup job", "err", err)
		return
	}
	slog.Info("queued scheduled device cleanup", "job_id", jobID)
}
