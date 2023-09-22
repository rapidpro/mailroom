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
	models.RegisterEventHandler(events.TypeContactStatusChanged, handleContactStatusChanged)
}

// handleContactStatusChanged updates contact status
func handleContactStatusChanged(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, oa *models.OrgAssets, scene *models.Scene, e flows.Event) error {
	event := e.(*events.ContactStatusChangedEvent)

	slog.Debug("contact status changed", "contact", scene.ContactUUID(), "session", scene.SessionID(), "status", event.Status)

	scene.AppendToEventPreCommitHook(hooks.CommitStatusChangesHook, event)
	scene.AppendToEventPostCommitHook(hooks.ContactModifiedHook, event)

	return nil
}
