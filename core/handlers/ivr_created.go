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
	"github.com/pkg/errors"
)

func init() {
	models.RegisterEventHandler(events.TypeIVRCreated, handleIVRCreated)
}

// handleIVRCreated creates the db msg for the passed in event
func handleIVRCreated(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, oa *models.OrgAssets, scene *models.Scene, e flows.Event) error {
	event := e.(*events.IVRCreatedEvent)

	slog.Debug("ivr created", "contact", scene.ContactUUID(), "session", scene.SessionID(), "text", event.Msg.Text())

	// get our call
	call := scene.Session().Call()
	if call == nil {
		return errors.Errorf("ivr session must have a call set")
	}

	// if our call is no longer in progress, return
	if call.Status() != models.CallStatusInProgress {
		return nil
	}

	msg := models.NewOutgoingIVR(rt.Config, oa.OrgID(), call, event.Msg, event.CreatedOn())

	// register to have this message committed
	scene.AppendToEventPreCommitHook(hooks.CommitIVRHook, msg)

	return nil
}
