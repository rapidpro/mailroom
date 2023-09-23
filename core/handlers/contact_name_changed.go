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
	models.RegisterEventHandler(events.TypeContactNameChanged, handleContactNameChanged)
}

// handleContactNameChanged changes the name of the contact
func handleContactNameChanged(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, oa *models.OrgAssets, scene *models.Scene, e flows.Event) error {
	event := e.(*events.ContactNameChangedEvent)

	slog.Debug("contact name changed", "contact", scene.ContactUUID(), "session", scene.SessionID(), "name", event.Name)

	scene.AppendToEventPreCommitHook(hooks.CommitNameChangesHook, event)
	scene.AppendToEventPostCommitHook(hooks.ContactModifiedHook, event)

	return nil
}
