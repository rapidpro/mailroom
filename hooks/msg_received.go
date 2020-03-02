package hooks

import (
	"context"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/models"
	"github.com/sirupsen/logrus"
)

func init() {
	models.RegisterEventHandler(events.TypeMsgReceived, handleMsgReceived)
}

// handleMsgReceived takes care of creating the incoming message for surveyor flows, it is a noop for all other flows
func handleMsgReceived(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, scene *models.Scene, e flows.Event) error {
	event := e.(*events.MsgReceivedEvent)

	// we only care about msg received events when dealing with surveyor flows
	if scene.Session().SessionType() != models.SurveyorFlow {
		return nil
	}

	logrus.WithFields(logrus.Fields{
		"contact_uuid": scene.ContactUUID(),
		"session_id":   scene.SessionID(),
		"text":         event.Msg.Text(),
		"urn":          event.Msg.URN(),
	}).Debug("msg received event")

	msg := models.NewIncomingMsg(org.OrgID(), nil, scene.ContactID(), &event.Msg, event.CreatedOn())

	// we'll commit this message with all the others
	scene.AppendToEventPreCommitHook(commitMessagesHook, msg)
	return nil
}
