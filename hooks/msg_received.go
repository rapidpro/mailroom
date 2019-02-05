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
	models.RegisterEventHook(events.TypeMsgReceived, handleMsgReceived)
}

// handleMsgMsgReceived takes care of creating the incoming message for surveyor flows
func handleMsgReceived(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, session *models.Session, e flows.Event) error {
	event := e.(*events.MsgReceivedEvent)

	// we only care about msg received events when dealing with surveyor flows
	if session.SessionType() != models.SurveyorFlow {
		return nil
	}

	logrus.WithFields(logrus.Fields{
		"contact_uuid": session.ContactUUID(),
		"session_id":   session.ID(),
		"text":         event.Msg.Text(),
		"urn":          event.Msg.URN(),
	}).Debug("msg received event")

	msg := models.NewIncomingMsg(org.OrgID(), nil, session.ContactID(), &event.Msg, event.CreatedOn())

	// register to have this message committed
	session.AddPreCommitEvent(commitMessagesHook, msg)
	return nil
}
