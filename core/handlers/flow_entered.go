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
	models.RegisterEventHandler(events.TypeFlowEntered, handleFlowEntered)
}

func handleFlowEntered(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, oa *models.OrgAssets, scene *models.Scene, e flows.Event) error {
	event := e.(*events.FlowEnteredEvent)

	slog.Debug("flow entered", "contact", scene.ContactUUID(), "session", scene.SessionID(), slog.Group("flow", "uuid", event.Flow.UUID, "name", event.Flow.Name))

	// we've potentially changed contact flow history.. only way to be sure would be loading contacts with their
	// flow history, but not sure that is worth it given how likely we are to be updating modified_on anyway
	scene.AppendToEventPreCommitHook(hooks.ContactModifiedHook, event)

	return nil
}
