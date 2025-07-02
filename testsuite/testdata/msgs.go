package testdata

import (
	"time"

	"github.com/lib/pq"
	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
)

type Label struct {
	ID   models.LabelID
	UUID assets.LabelUUID
}

// InsertIncomingMsg inserts an incoming text message
func InsertIncomingMsg(rt *runtime.Runtime, org *Org, channel *Channel, contact *Contact, text string, status models.MsgStatus) *flows.MsgIn {
	msgUUID := flows.MsgUUID(uuids.New())
	var id flows.MsgID
	must(rt.DB.Get(&id,
		`INSERT INTO msgs_msg(uuid, text, created_on, direction, msg_type, status, visibility, msg_count, error_count, next_attempt, contact_id, contact_urn_id, org_id, channel_id)
	  	 VALUES($1, $2, NOW(), 'I', $3, $4, 'V', 1, 0, NOW(), $5, $6, $7, $8) RETURNING id`, msgUUID, text, models.MsgTypeText, status, contact.ID, contact.URNID, org.ID, channel.ID,
	))

	msg := flows.NewMsgIn(msgUUID, contact.URN, assets.NewChannelReference(channel.UUID, ""), text, nil)
	msg.SetID(id)
	return msg
}

// InsertOutgoingMsg inserts an outgoing text message
func InsertOutgoingMsg(rt *runtime.Runtime, org *Org, channel *Channel, contact *Contact, text string, attachments []utils.Attachment, status models.MsgStatus, highPriority bool) *flows.MsgOut {
	return insertOutgoingMsg(rt, org, channel, contact, text, attachments, envs.Locale(`eng-US`), models.MsgTypeText, status, highPriority, 0, nil)
}

// InsertErroredOutgoingMsg inserts an ERRORED(E) outgoing text message
func InsertErroredOutgoingMsg(rt *runtime.Runtime, org *Org, channel *Channel, contact *Contact, text string, errorCount int, nextAttempt time.Time, highPriority bool) *flows.MsgOut {
	return insertOutgoingMsg(rt, org, channel, contact, text, nil, envs.NilLocale, models.MsgTypeText, models.MsgStatusErrored, highPriority, errorCount, &nextAttempt)
}

func insertOutgoingMsg(rt *runtime.Runtime, org *Org, channel *Channel, contact *Contact, text string, attachments []utils.Attachment, locale envs.Locale, typ models.MsgType, status models.MsgStatus, highPriority bool, errorCount int, nextAttempt *time.Time) *flows.MsgOut {
	var channelRef *assets.ChannelReference
	var channelID models.ChannelID
	if channel != nil {
		channelRef = assets.NewChannelReference(channel.UUID, "")
		channelID = channel.ID
	}

	msg := flows.NewMsgOut(contact.URN, channelRef, text, attachments, nil, nil, flows.NilMsgTopic, envs.NilLocale, flows.NilUnsendableReason)

	var sentOn *time.Time
	if status == models.MsgStatusWired || status == models.MsgStatusSent || status == models.MsgStatusDelivered {
		t := dates.Now()
		sentOn = &t
	}

	var id flows.MsgID
	must(rt.DB.Get(&id,
		`INSERT INTO msgs_msg(uuid, text, attachments, locale, created_on, direction, msg_type, status, visibility, contact_id, contact_urn_id, org_id, channel_id, sent_on, msg_count, error_count, next_attempt, high_priority)
	  	 VALUES($1, $2, $3, $4, NOW(), 'O', $5, $6, 'V', $7, $8, $9, $10, $11, 1, $12, $13, $14) RETURNING id`,
		msg.UUID(), text, pq.Array(attachments), locale, typ, status, contact.ID, contact.URNID, org.ID, channelID, sentOn, errorCount, nextAttempt, highPriority,
	))
	msg.SetID(id)
	return msg
}

func InsertBroadcast(rt *runtime.Runtime, org *Org, baseLanguage envs.Language, text map[envs.Language]string, schedID models.ScheduleID, contacts []*Contact, groups []*Group) models.BroadcastID {
	translations := make(flows.BroadcastTranslations)
	for lang, t := range text {
		translations[lang] = &flows.BroadcastTranslation{Text: t}
	}

	var id models.BroadcastID
	must(rt.DB.Get(&id,
		`INSERT INTO msgs_broadcast(org_id, base_language, translations, schedule_id, status, created_on, modified_on, created_by_id, modified_by_id, is_active)
		VALUES($1, $2, $3, $4, 'P', NOW(), NOW(), 1, 1, TRUE) RETURNING id`, org.ID, baseLanguage, translations, schedID,
	))

	for _, contact := range contacts {
		rt.DB.MustExec(`INSERT INTO msgs_broadcast_contacts(broadcast_id, contact_id) VALUES($1, $2)`, id, contact.ID)
	}
	for _, group := range groups {
		rt.DB.MustExec(`INSERT INTO msgs_broadcast_groups(broadcast_id, contactgroup_id) VALUES($1, $2)`, id, group.ID)
	}

	return id
}
