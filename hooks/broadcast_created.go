package hooks

import (
	"context"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/queue"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func init() {
	models.RegisterEventHandler(events.TypeBroadcastCreated, handleBroadcastCreated)
}

// StartBroadcastsHook is our hook for starting the broadcasts created in these scene
type StartBroadcastsHook struct{}

var startBroadcastsHook = &StartBroadcastsHook{}

// Apply queues up our broadcasts for sending
func (h *StartBroadcastsHook) Apply(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, scenes map[*models.Scene][]interface{}) error {
	rc := rp.Get()
	defer rc.Close()

	// for each of our scene
	for _, es := range scenes {
		for _, e := range es {
			event := e.(*events.BroadcastCreatedEvent)

			bcast, err := models.NewBroadcastFromEvent(ctx, tx, org, event)
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

			err = queue.AddTask(rc, taskQ, queue.SendBroadcast, int(org.OrgID()), bcast, priority)
			if err != nil {
				return errors.Wrapf(err, "error queuing broadcast")
			}
		}
	}

	return nil
}

// handleBroadcastCreated is called for each broadcast created event across our scene
func handleBroadcastCreated(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, scene *models.Scene, e flows.Event) error {
	event := e.(*events.BroadcastCreatedEvent)
	logrus.WithFields(logrus.Fields{
		"contact_uuid": scene.ContactUUID(),
		"session_id":   scene.SessionID(),
		"translations": event.Translations[event.BaseLanguage],
	}).Debug("broadcast created")

	// schedule this for being started after our scene are committed
	scene.AppendToEventPostCommitHook(startBroadcastsHook, event)

	return nil
}
