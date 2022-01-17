package testdata

import (
	"database/sql"
	"time"

	"github.com/lib/pq"
	"github.com/lib/pq/hstore"
	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/envs"
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
func InsertOutgoingMsg(db *sqlx.DB, org *Org, channel *Channel, contact *Contact, text string, attachments []utils.Attachment, status models.MsgStatus, highPriority bool) *flows.MsgOut {
	return insertOutgoingMsg(db, org, channel, contact, text, attachments, status, highPriority, 0, nil)
}

// InsertErroredOutgoingMsg inserts an ERRORED(E) outgoing message
func InsertErroredOutgoingMsg(db *sqlx.DB, org *Org, channel *Channel, contact *Contact, text string, errorCount int, nextAttempt time.Time, highPriority bool) *flows.MsgOut {
	return insertOutgoingMsg(db, org, channel, contact, text, nil, models.MsgStatusErrored, highPriority, errorCount, &nextAttempt)
}

func insertOutgoingMsg(db *sqlx.DB, org *Org, channel *Channel, contact *Contact, text string, attachments []utils.Attachment, status models.MsgStatus, highPriority bool, errorCount int, nextAttempt *time.Time) *flows.MsgOut {
	var channelRef *assets.ChannelReference
	var channelID models.ChannelID
	if channel != nil {
		channelRef = assets.NewChannelReference(channel.UUID, "")
		channelID = channel.ID
	}

	msg := flows.NewMsgOut(contact.URN, channelRef, text, attachments, nil, nil, flows.NilMsgTopic)

	var sentOn *time.Time
	if status == models.MsgStatusWired || status == models.MsgStatusSent || status == models.MsgStatusDelivered {
		t := dates.Now()
		sentOn = &t
	}

	var id flows.MsgID
	must(db.Get(&id,
		`INSERT INTO msgs_msg(uuid, text, attachments, created_on, direction, status, visibility, contact_id, contact_urn_id, org_id, channel_id, sent_on, msg_count, error_count, next_attempt, high_priority)
	  	 VALUES($1, $2, $3, NOW(), 'O', $4, 'V', $5, $6, $7, $8, $9, 1, $10, $11, $12) RETURNING id`,
		msg.UUID(), text, pq.Array(attachments), status, contact.ID, contact.URNID, org.ID, channelID, sentOn, errorCount, nextAttempt, highPriority,
	))
	msg.SetID(id)
	return msg
}

func InsertBroadcast(db *sqlx.DB, org *Org, baseLanguage envs.Language, text map[envs.Language]string, schedID models.ScheduleID, contacts []*Contact, groups []*Group) models.BroadcastID {
	textMap := make(map[string]sql.NullString, len(text))
	for lang, t := range text {
		textMap[string(lang)] = sql.NullString{String: t, Valid: true}
	}

	var id models.BroadcastID
	must(db.Get(&id,
		`INSERT INTO msgs_broadcast(org_id, base_language, text, schedule_id, status, send_all, created_on, modified_on, created_by_id, modified_by_id)
		VALUES($1, $2, $3, $4, 'P', TRUE, NOW(), NOW(), 1, 1) RETURNING id`, org.ID, baseLanguage, hstore.Hstore{Map: textMap}, schedID,
	))

	for _, contact := range contacts {
		db.MustExec(`INSERT INTO msgs_broadcast_contacts(broadcast_id, contact_id) VALUES($1, $2)`, id, contact.ID)
	}
	for _, group := range groups {
		db.MustExec(`INSERT INTO msgs_broadcast_groups(broadcast_id, contactgroup_id) VALUES($1, $2)`, id, group.ID)
	}

	return id
}
