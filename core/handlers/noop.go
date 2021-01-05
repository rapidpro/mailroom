package handlers

import (
	"context"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/core/models"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
)

func init() {
	models.RegisterEventHandler(events.TypeContactRefreshed, NoopHandler)
	models.RegisterEventHandler(events.TypeEnvironmentRefreshed, NoopHandler)
	models.RegisterEventHandler(events.TypeError, NoopHandler)
	models.RegisterEventHandler(events.TypeFailure, NoopHandler)
	models.RegisterEventHandler(events.TypeFlowEntered, NoopHandler)
	models.RegisterEventHandler(events.TypeMsgWait, NoopHandler)
	models.RegisterEventHandler(events.TypeRunExpired, NoopHandler)
	models.RegisterEventHandler(events.TypeRunResultChanged, NoopHandler)
	models.RegisterEventHandler(events.TypeWaitTimedOut, NoopHandler)
}

// NoopHandler is our hook for events we ignore in a run
func NoopHandler(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, oa *models.OrgAssets, scene *models.Scene, event flows.Event) error {
	return nil
}
