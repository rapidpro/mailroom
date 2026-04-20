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
	models.RegisterEventHandler(events.TypeContactLanguageChanged, handleContactLanguageChanged)
}

// handleContactLanguageChanged is called when we process a contact language change
func handleContactLanguageChanged(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, oa *models.OrgAssets, scene *models.Scene, e flows.Event) error {
	event := e.(*events.ContactLanguageChangedEvent)

	slog.Debug("contact language changed", "contact", scene.ContactUUID(), "session", scene.SessionID(), "language", event.Language)

	scene.AppendToEventPreCommitHook(hooks.CommitLanguageChangesHook, event)
	scene.AppendToEventPostCommitHook(hooks.ContactModifiedHook, event)

	return nil
}
