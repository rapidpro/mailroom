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
	models.RegisterEventHandler(events.TypeResthookCalled, handleResthookCalled)
}

// handleResthookCalled is called for each resthook call in a scene
func handleResthookCalled(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, oa *models.OrgAssets, scene *models.Scene, e flows.Event) error {
	event := e.(*events.ResthookCalledEvent)

	slog.Debug("resthook called", "contact", scene.ContactUUID(), "session", scene.SessionID(), "resthook", event.Resthook)

	// look up our resthook id
	resthook := oa.ResthookBySlug(event.Resthook)
	if resthook == nil {
		slog.Warn("unable to find resthook with slug, ignoring event", "resthook", event.Resthook)
		return nil
	}

	// create an event for this call
	re := models.NewWebhookEvent(
		oa.OrgID(),
		resthook.ID(),
		string(event.Payload),
		event.CreatedOn(),
	)
	scene.AppendToEventPreCommitHook(hooks.InsertWebhookEventHook, re)

	return nil
}
