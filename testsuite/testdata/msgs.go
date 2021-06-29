package testdata

import (
	"github.com/lib/pq"
	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/core/models"

	"github.com/jmoiron/sqlx"
)

type Label struct {
	ID   models.LabelID
	UUID assets.LabelUUID
}

// InsertIncomingMsg inserts an incoming message
func InsertIncomingMsg(db *sqlx.DB, org *Org, channel *Channel, contact *Contact, text string, status models.MsgStatus) *flows.MsgIn {
	msgUUID := flows.MsgUUID(uuids.New())
	var id flows.MsgID
	must(db.Get(&id,
		`INSERT INTO msgs_msg(uuid, text, created_on, direction, status, visibility, msg_count, error_count, next_attempt, contact_id, contact_urn_id, org_id, channel_id)
	  	 VALUES($1, $2, NOW(), 'I', $3, 'V', 1, 0, NOW(), $4, $5, $6, $7) RETURNING id`, msgUUID, text, status, contact.ID, contact.URNID, org.ID, channel.ID,
	))

	msg := flows.NewMsgIn(msgUUID, contact.URN, assets.NewChannelReference(channel.UUID, ""), text, nil)
	msg.SetID(id)
	return msg
}

// InsertOutgoingMsg inserts an outgoing message
func InsertOutgoingMsg(db *sqlx.DB, org *Org, channel *Channel, contact *Contact, text string, attachments []utils.Attachment, status models.MsgStatus) *flows.MsgOut {
	msg := flows.NewMsgOut(contact.URN, assets.NewChannelReference(channel.UUID, ""), text, attachments, nil, nil, flows.NilMsgTopic)

	var id flows.MsgID
	must(db.Get(&id,
		`INSERT INTO msgs_msg(uuid, text, attachments, created_on, direction, status, visibility, msg_count, error_count, next_attempt, contact_id, contact_urn_id, org_id, channel_id)
	  	 VALUES($1, $2, $3, NOW(), 'O', $4, 'V', 1, 0, NOW(), $5, $6, $7, $8) RETURNING id`, msg.UUID(), text, pq.Array(attachments), status, contact.ID, contact.URNID, org.ID, channel.ID,
	))
	msg.SetID(id)
	return msg
}
