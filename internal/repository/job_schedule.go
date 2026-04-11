package repository

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stevenvi/bokeh-mediaserver/internal/utils"
)

// JobSchedule represents a row in the jobs_schedule table.
type JobSchedule struct {
	Name        string    `json:"name"`
	Cron        string    `json:"cron"`
	Description *string   `json:"description,omitempty"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// JobScheduleList returns all job schedule rows.
func JobScheduleList(ctx context.Context, db utils.DBTX) ([]*JobSchedule, error) {
	rows, err := db.Query(ctx, `SELECT name, cron, description, updated_at FROM jobs_schedule ORDER BY name`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToAddrOfStructByPos[JobSchedule])
}

// JobScheduleUpsert inserts or updates a schedule row.
func JobScheduleUpsert(ctx context.Context, db utils.DBTX, name, cron string, description *string) error {
	_, err := db.Exec(ctx,
		`INSERT INTO jobs_schedule (name, cron, description, updated_at)
		 VALUES ($1, $2, $3, now())
		 ON CONFLICT (name) DO UPDATE SET
		     cron        = EXCLUDED.cron,
		     description = EXCLUDED.description,
		     updated_at  = now()`,
		name, cron, description,
	)
	return err
}

// JobScheduleDelete removes a schedule row. Returns true if a row was deleted.
func JobScheduleDelete(ctx context.Context, db utils.DBTX, name string) (bool, error) {
	tag, err := db.Exec(ctx, `DELETE FROM jobs_schedule WHERE name = $1`, name)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}
