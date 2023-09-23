package handlers

import (
	"context"
	"log/slog"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/core/hooks"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
)

func init() {
	models.RegisterEventHandler(events.TypeContactFieldChanged, handleContactFieldChanged)
}

// handleContactFieldChanged is called when a contact field changes
func handleContactFieldChanged(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, oa *models.OrgAssets, scene *models.Scene, e flows.Event) error {
	event := e.(*events.ContactFieldChangedEvent)

	slog.Debug("contact field changed", "contact", scene.ContactUUID(), "session", scene.SessionID(), "field", event.Field.Key, "value", event.Value)

	scene.AppendToEventPreCommitHook(hooks.CommitFieldChangesHook, event)
	scene.AppendToEventPreCommitHook(hooks.UpdateCampaignEventsHook, event)
	scene.AppendToEventPostCommitHook(hooks.ContactModifiedHook, event)

	return nil
}
