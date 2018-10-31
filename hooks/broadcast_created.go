package hooks

import (
	"context"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/queue"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func init() {
	models.RegisterEventHook(events.TypeBroadcastCreated, handleBroadcastCreated)
}

// StartBroadcastsHook is our hook for starting the broadcasts created in these sessions
type StartBroadcastsHook struct{}

var startBroadcastsHook = &StartBroadcastsHook{}

// Apply queues up our broadcasts for sending
func (h *StartBroadcastsHook) Apply(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, sessions map[*models.Session][]interface{}) error {
	rc := rp.Get()
	defer rc.Close()

	// for each of our sessions
	for s, es := range sessions {
		for _, e := range es {
			event := e.(*events.BroadcastCreatedEvent)

			// we skip over any session starts that involve groups if we are in a batch start
			if len(sessions) > 1 && len(event.Groups) > 0 {
				logrus.WithField("session_id", s.ID).Error("ignoring broadcast on group in batch")
				continue
			}

			bcast, err := models.NewBroadcastFromEvent(ctx, tx, org, event)
			if err != nil {
				return errors.Wrapf(err, "error creating broadcast")
			}

			taskQ := mailroom.HandlerQueue
			priority := queue.DefaultPriority

			// if we are starting groups, queue to our batch queue instead, but with high priority
			// TODO: this probably isn't enough to guarantee instant execution
			if len(bcast.GroupIDs()) > 0 {
				taskQ = mailroom.BatchQueue
				priority = queue.HighPriority
			}

			err = queue.AddTask(rc, taskQ, mailroom.SendBroadcastType, int(org.OrgID()), bcast, priority)
			if err != nil {
				return errors.Wrapf(err, "error queuing broadcast")
			}
		}
	}

	return nil
}

// handleBroadcastCreated is called for each broadcast created event across our sessions
func handleBroadcastCreated(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, session *models.Session, e flows.Event) error {
	event := e.(*events.BroadcastCreatedEvent)
	logrus.WithFields(logrus.Fields{
		"contact_uuid": session.ContactUUID(),
		"session_id":   session.ID,
		"translations": event.Translations,
	}).Debug("broadcast created")

	// schedule this for being started after our sessions are committed
	session.AddPostCommitEvent(startBroadcastsHook, event)

	return nil
}
