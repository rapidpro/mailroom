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
	models.RegisterEventHandler(events.TypeContactFieldChanged, handleContactFieldChanged)
}

// handleContactFieldChanged is called when a contact field changes
func handleContactFieldChanged(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, oa *models.OrgAssets, scene *models.Scene, e flows.Event) error {
	event := e.(*events.ContactFieldChangedEvent)
	logrus.WithFields(logrus.Fields{
		"contact_uuid": scene.ContactUUID(),
		"session_id":   scene.SessionID(),
		"field_key":    event.Field.Key,
		"value":        event.Value,
	}).Debug("contact field changed")

	// add our callback
	scene.AppendToEventPreCommitHook(hooks.CommitFieldChangesHook, event)
	scene.AppendToEventPreCommitHook(hooks.UpdateCampaignEventsHook, event)

	return nil
}
