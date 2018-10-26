package hooks

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
	models.RegisterEventHook(events.TypeSessionTriggered, handleSessionTriggered)
}

// StartSessionsHook is our hook to fire our session starts
type StartSessionsHook struct{}

var startSessionsHook = &StartSessionsHook{}

// Apply queues up our flow starts
func (h *StartSessionsHook) Apply(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, sessions map[*models.Session][]interface{}) error {

	return nil
}

// handleSessionTriggered queues this event for being started after our sessions are committed
func handleSessionTriggered(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, session *models.Session, e flows.Event) error {
	event := e.(*events.SessionTriggeredEvent)
	logrus.WithFields(logrus.Fields{
		"contact_uuid": session.ContactUUID(),
		"session_id":   session.ID,
		"flow":         event.Flow.Name,
		"flow_uuid":    event.Flow.UUID,
	}).Debug("session triggered")

	// schedule this for being started after our sessions are committed
	session.AddPostCommitEvent(startSessionsHook, event)

	return nil
}
