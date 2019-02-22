package hooks

import (
	"context"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/models"
)

func init() {
	models.RegisterEventHook(events.TypeEnvironmentRefreshed, NoopHandler)
	models.RegisterEventHook(events.TypeContactRefreshed, NoopHandler)
	models.RegisterEventHook(events.TypeError, NoopHandler)
	models.RegisterEventHook(events.TypeRunResultChanged, NoopHandler)
	models.RegisterEventHook(events.TypeWaitTimedOut, NoopHandler)
	models.RegisterEventHook(events.TypeRunExpired, NoopHandler)
	models.RegisterEventHook(events.TypeFlowEntered, NoopHandler)
	models.RegisterEventHook(events.TypeMsgWait, NoopHandler)
}

// NoopHandler is our hook for events we ignore in a run
func NoopHandler(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, session *models.Session, event flows.Event) error {
	return nil
}
