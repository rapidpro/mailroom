package hooks

import (
	"context"

	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/queue"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

// StartBroadcastsHook is our hook for starting broadcasts
var StartBroadcastsHook models.EventCommitHook = &startBroadcastsHook{}

type startBroadcastsHook struct{}

// Apply queues up our broadcasts for sending
func (h *startBroadcastsHook) Apply(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, oa *models.OrgAssets, scenes map[*models.Scene][]interface{}) error {
	rc := rp.Get()
	defer rc.Close()

	// for each of our scene
	for _, es := range scenes {
		for _, e := range es {
			event := e.(*events.BroadcastCreatedEvent)

			bcast, err := models.NewBroadcastFromEvent(ctx, tx, oa, event)
			if err != nil {
				return errors.Wrapf(err, "error creating broadcast")
			}

			taskQ := queue.HandlerQueue
			priority := queue.DefaultPriority

			// if we are starting groups, queue to our batch queue instead, but with high priority
			if len(bcast.GroupIDs()) > 0 {
				taskQ = queue.BatchQueue
				priority = queue.HighPriority
			}

			err = queue.AddTask(rc, taskQ, queue.SendBroadcast, int(oa.OrgID()), bcast, priority)
			if err != nil {
				return errors.Wrapf(err, "error queuing broadcast")
			}
		}
	}

	return nil
}
