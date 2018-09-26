package handlers

import (
	"context"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/models"
	"github.com/sirupsen/logrus"
)

func init() {
	models.RegisterEventHandler(events.TypeEnvironmentChanged, NoopHandler)
	models.RegisterEventHandler(events.TypeError, NoopHandler)
	models.RegisterEventHandler(events.TypeMsgReceived, NoopHandler)
	models.RegisterEventHandler(events.TypeRunResultChanged, NoopHandler)
	models.RegisterEventHandler(events.TypeContactChanged, NoopHandler)
	models.RegisterEventHandler(events.TypeWebhookCalled, NoopHandler)
	models.RegisterEventHandler(events.TypeWaitTimedOut, NoopHandler)
	models.RegisterEventHandler(events.TypeRunExpired, NoopHandler)
}

// NoopHandler is our handler for events we ignore in a run
func NoopHandler(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, session *models.Session, event flows.Event) error {
	logrus.WithFields(logrus.Fields{
		"event_type":   event.Type(),
		"contact_uuid": session.ContactUUID(),
	}).Debug("ignoring event")
	return nil
}
