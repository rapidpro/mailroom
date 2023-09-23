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
	models.RegisterEventHandler(events.TypeSessionTriggered, handleSessionTriggered)
}

func handleSessionTriggered(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, oa *models.OrgAssets, scene *models.Scene, e flows.Event) error {
	event := e.(*events.SessionTriggeredEvent)

	slog.Debug("session triggered", "contact", scene.ContactUUID(), "session", scene.SessionID(), slog.Group("flow", "uuid", event.Flow.UUID, "name", event.Flow.Name))

	scene.AppendToEventPreCommitHook(hooks.InsertStartHook, event)

	return nil
}
