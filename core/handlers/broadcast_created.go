package handlers

import (
	"context"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/core/hooks"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"
)

func init() {
	models.RegisterEventHandler(events.TypeBroadcastCreated, handleBroadcastCreated)
}

// handleBroadcastCreated is called for each broadcast created event across our scene
func handleBroadcastCreated(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, oa *models.OrgAssets, scene *models.Scene, e flows.Event) error {
	event := e.(*events.BroadcastCreatedEvent)
	logrus.WithFields(logrus.Fields{
		"contact_uuid": scene.ContactUUID(),
		"session_id":   scene.SessionID(),
		"translations": event.Translations[event.BaseLanguage],
	}).Debug("broadcast created")

	// schedule this for being started after our scene are committed
	scene.AppendToEventPostCommitHook(hooks.StartBroadcastsHook, event)

	return nil
}
