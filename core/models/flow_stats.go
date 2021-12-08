package models

import (
	"context"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/flows"
)

func RecordFlowStatistics(ctx context.Context, tx *sqlx.Tx, sessions []flows.Session, sprints []flows.Sprint) error {
	// TODO
	return nil
}
