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
