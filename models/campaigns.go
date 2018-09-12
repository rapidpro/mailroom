package models

import (
	"context"
	"time"

	"github.com/jmoiron/sqlx"
)

func MarkCampaignEventFired(ctx context.Context, db *sqlx.DB, fireID int, fired time.Time) error {
	_, err := db.ExecContext(ctx, markEventFired, fireID, fired)
	return err
}

const markEventFired = `
UPDATE 
	campaigns_eventfire
SET 
	fired = $2
WHERE
	id = $1
`
