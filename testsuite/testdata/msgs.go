package testdata

import (
	"github.com/lib/pq"
	"github.com/nyaruka/gocommon/urns"
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
func InsertIncomingMsg(db *sqlx.DB, org *Org, contactID models.ContactID, urn urns.URN, urnID models.URNID, text string) *flows.MsgIn {
	msgUUID := flows.MsgUUID(uuids.New())
	var id flows.MsgID
	must(db.Get(&id,
		`INSERT INTO msgs_msg(uuid, text, created_on, direction, status, visibility, msg_count, error_count, next_attempt, contact_id, contact_urn_id, org_id)
	  	 VALUES($1, $2, NOW(), 'I', 'P', 'V', 1, 0, NOW(), $3, $4, $5) RETURNING id`, msgUUID, text, contactID, urnID, org.ID,
	))

	msg := flows.NewMsgIn(msgUUID, urn, nil, text, nil)
	msg.SetID(id)
	return msg
}

// InsertOutgoingMsg inserts an outgoing message
func InsertOutgoingMsg(db *sqlx.DB, org *Org, contactID models.ContactID, urn urns.URN, urnID models.URNID, text string, attachments []utils.Attachment) *flows.MsgOut {
	msg := flows.NewMsgOut(urn, nil, text, nil, nil, nil, flows.NilMsgTopic)

	var id flows.MsgID
	must(db.Get(&id,
		`INSERT INTO msgs_msg(uuid, text, attachments, created_on, direction, status, visibility, msg_count, error_count, next_attempt, contact_id, contact_urn_id, org_id)
	  	 VALUES($1, $2, $3, NOW(), 'O', 'P', 'V', 1, 0, NOW(), $4, $5, $6) RETURNING id`, msg.UUID(), text, pq.Array(attachments), contactID, urnID, org.ID,
	))
	msg.SetID(id)
	return msg
}
