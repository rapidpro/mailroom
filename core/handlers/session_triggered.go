package handlers

import (
	"context"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/core/hooks"
	"github.com/nyaruka/mailroom/core/models"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"
)

func init() {
	models.RegisterEventHandler(events.TypeSessionTriggered, handleSessionTriggered)
}

// handleSessionTriggered queues this event for being started after our scene are committed
func handleSessionTriggered(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, oa *models.OrgAssets, scene *models.Scene, e flows.Event) error {
	event := e.(*events.SessionTriggeredEvent)

	logrus.WithFields(logrus.Fields{
		"contact_uuid": scene.ContactUUID(),
		"session_id":   scene.SessionID(),
		"flow":         event.Flow.Name,
		"flow_uuid":    event.Flow.UUID,
	}).Debug("scene triggered")

	scene.AppendToEventPreCommitHook(hooks.InsertStartHook, event)

	return nil
}
