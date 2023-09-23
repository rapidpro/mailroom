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
	models.RegisterEventHandler(events.TypeMsgReceived, handleMsgReceived)
}

// handleMsgReceived takes care of creating the incoming message for surveyor flows, it is a noop for all other flows
func handleMsgReceived(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, oa *models.OrgAssets, scene *models.Scene, e flows.Event) error {
	event := e.(*events.MsgReceivedEvent)

	slog.Debug("msg received", "contact", scene.ContactUUID(), "session", scene.SessionID(), "text", event.Msg.Text(), "urn", event.Msg.URN())

	// for surveyor sessions we need to actually create the message
	if scene.Session() != nil && scene.Session().SessionType() == models.FlowTypeSurveyor {
		msg := models.NewIncomingSurveyorMsg(rt.Config, oa.OrgID(), nil, scene.ContactID(), &event.Msg, event.CreatedOn())

		// we'll commit this message with all the others
		scene.AppendToEventPreCommitHook(hooks.CommitMessagesHook, msg)
	}

	// update the contact's last seen date
	scene.AppendToEventPreCommitHook(hooks.ContactLastSeenHook, event)
	scene.AppendToEventPreCommitHook(hooks.UpdateCampaignEventsHook, event)

	return nil
}
