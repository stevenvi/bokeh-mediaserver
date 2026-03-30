package repository

import (
	"context"

	"github.com/stevenvi/bokeh-mediaserver/internal/utils"
)

// ServerConfigTranscodeBitrate reads the transcode_bitrate_kbps value from server_config.
func ServerConfigTranscodeBitrate(ctx context.Context, db utils.DBTX) (int, error) {
	var kbps int
	err := db.QueryRow(ctx,
		`SELECT transcode_bitrate_kbps FROM server_config WHERE id = 1`,
	).Scan(&kbps)
	return kbps, err
}

// ServerConfigSchedules reads cron schedules from server_config. Returns a map of
// config column name → nullable schedule string.
func ServerConfigSchedules(ctx context.Context, db utils.DBTX) (map[string]*string, error) {
	var scanSched, integritySched, deviceCleanupSched, coverCycleSched *string
	err := db.QueryRow(ctx,
		`SELECT scan_schedule, integrity_schedule, device_cleanup_schedule, cover_cycle_schedule FROM server_config WHERE id = 1`,
	).Scan(&scanSched, &integritySched, &deviceCleanupSched, &coverCycleSched)
	if err != nil {
		return nil, err
	}
	return map[string]*string{
		"scan_schedule":           scanSched,
		"integrity_schedule":      integritySched,
		"device_cleanup_schedule": deviceCleanupSched,
		"cover_cycle_schedule":    coverCycleSched,
	}, nil
}
