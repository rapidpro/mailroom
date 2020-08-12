package hooks

import (
	"context"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/models"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"
)

func init() {
	models.RegisterEventHandler(events.TypeMsgReceived, handleMsgReceived)
}

// handleMsgReceived takes care of creating the incoming message for surveyor flows, it is a noop for all other flows
func handleMsgReceived(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, oa *models.OrgAssets, scene *models.Scene, e flows.Event) error {
	event := e.(*events.MsgReceivedEvent)

	// for surveyor sessions we need to actually create the message
	if scene.Session() != nil && scene.Session().SessionType() == models.SurveyorFlow {
		logrus.WithFields(logrus.Fields{
			"contact_uuid": scene.ContactUUID(),
			"session_id":   scene.SessionID(),
			"text":         event.Msg.Text(),
			"urn":          event.Msg.URN(),
		}).Debug("msg received event")

		msg := models.NewIncomingMsg(oa.OrgID(), nil, scene.ContactID(), &event.Msg, event.CreatedOn())

		// we'll commit this message with all the others
		scene.AppendToEventPreCommitHook(commitMessagesHook, msg)
	}

	// update the contact's last seen date
	scene.AppendToEventPreCommitHook(contactLastSeenHook, event)
	scene.AppendToEventPreCommitHook(updateCampaignEventsHook, event)

	return nil
}
