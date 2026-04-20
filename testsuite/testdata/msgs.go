package testdata

import (
	"time"

	"github.com/lib/pq"
	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/gocommon/i18n"
	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
)

type Msg struct {
	ID models.MsgID
}

type MsgIn struct {
	Msg
	FlowMsg *flows.MsgIn
}

func (m *MsgIn) Label(rt *runtime.Runtime, labels ...*Label) {
	for _, l := range labels {
		rt.DB.MustExec(`INSERT INTO msgs_msg_labels(msg_id, label_id) VALUES($1, $2)`, m.ID, l.ID)
	}
}

type MsgOut struct {
	Msg
	FlowMsg *flows.MsgOut
}

type Label struct {
	ID   models.LabelID
	UUID assets.LabelUUID
}

type OptIn struct {
	ID   models.OptInID
	UUID assets.OptInUUID
}

// InsertIncomingMsg inserts an incoming text message
func InsertIncomingMsg(rt *runtime.Runtime, org *Org, channel *Channel, contact *Contact, text string, status models.MsgStatus) *MsgIn {
	msgUUID := flows.MsgUUID(uuids.New())
	var id models.MsgID
	must(rt.DB.Get(&id,
		`INSERT INTO msgs_msg(uuid, text, created_on, direction, msg_type, status, visibility, msg_count, error_count, next_attempt, contact_id, contact_urn_id, org_id, channel_id)
	  	 VALUES($1, $2, NOW(), 'I', $3, $4, 'V', 1, 0, NOW(), $5, $6, $7, $8) RETURNING id`, msgUUID, text, models.MsgTypeText, status, contact.ID, contact.URNID, org.ID, channel.ID,
	))

	fm := flows.NewMsgIn(msgUUID, contact.URN, assets.NewChannelReference(channel.UUID, ""), text, nil)
	fm.SetID(flows.MsgID(id))
	return &MsgIn{Msg: Msg{ID: id}, FlowMsg: fm}
}

// InsertOutgoingMsg inserts an outgoing text message
func InsertOutgoingMsg(rt *runtime.Runtime, org *Org, channel *Channel, contact *Contact, text string, attachments []utils.Attachment, status models.MsgStatus, highPriority bool) *MsgOut {
	return insertOutgoingMsg(rt, org, channel, contact, text, attachments, i18n.Locale(`eng-US`), models.MsgTypeText, status, highPriority, 0, nil)
}

// InsertErroredOutgoingMsg inserts an ERRORED(E) outgoing text message
func InsertErroredOutgoingMsg(rt *runtime.Runtime, org *Org, channel *Channel, contact *Contact, text string, errorCount int, nextAttempt time.Time, highPriority bool) *MsgOut {
	return insertOutgoingMsg(rt, org, channel, contact, text, nil, i18n.NilLocale, models.MsgTypeText, models.MsgStatusErrored, highPriority, errorCount, &nextAttempt)
}

func insertOutgoingMsg(rt *runtime.Runtime, org *Org, channel *Channel, contact *Contact, text string, attachments []utils.Attachment, locale i18n.Locale, typ models.MsgType, status models.MsgStatus, highPriority bool, errorCount int, nextAttempt *time.Time) *MsgOut {
	var channelRef *assets.ChannelReference
	var channelID models.ChannelID
	if channel != nil {
		channelRef = assets.NewChannelReference(channel.UUID, "")
		channelID = channel.ID
	}

	var sentOn *time.Time
	if status == models.MsgStatusWired || status == models.MsgStatusSent || status == models.MsgStatusDelivered {
		t := dates.Now()
		sentOn = &t
	}

	fm := flows.NewMsgOut(contact.URN, channelRef, text, attachments, nil, nil, flows.NilMsgTopic, i18n.NilLocale, flows.NilUnsendableReason)

	var id models.MsgID
	must(rt.DB.Get(&id,
		`INSERT INTO msgs_msg(uuid, text, attachments, locale, created_on, direction, msg_type, status, visibility, contact_id, contact_urn_id, org_id, channel_id, sent_on, msg_count, error_count, next_attempt, high_priority)
	  	 VALUES($1, $2, $3, $4, NOW(), 'O', $5, $6, 'V', $7, $8, $9, $10, $11, 1, $12, $13, $14) RETURNING id`,
		fm.UUID(), text, pq.Array(attachments), locale, typ, status, contact.ID, contact.URNID, org.ID, channelID, sentOn, errorCount, nextAttempt, highPriority,
	))

	fm.SetID(flows.MsgID(id))
	return &MsgOut{Msg: Msg{ID: id}, FlowMsg: fm}
}

func InsertBroadcast(rt *runtime.Runtime, org *Org, baseLanguage i18n.Language, text map[i18n.Language]string, optIn *OptIn, schedID models.ScheduleID, contacts []*Contact, groups []*Group) models.BroadcastID {
	translations := make(flows.BroadcastTranslations)
	for lang, t := range text {
		translations[lang] = &flows.BroadcastTranslation{Text: t}
	}

	var optInID models.OptInID
	if optIn != nil {
		optInID = optIn.ID
	}

	var id models.BroadcastID
	must(rt.DB.Get(&id,
		`INSERT INTO msgs_broadcast(org_id, base_language, translations, optin_id, schedule_id, status, created_on, modified_on, created_by_id, modified_by_id, is_active)
		VALUES($1, $2, $3, $4, $5, 'P', NOW(), NOW(), 1, 1, TRUE) RETURNING id`, org.ID, baseLanguage, translations, optInID, schedID,
	))

	for _, contact := range contacts {
		rt.DB.MustExec(`INSERT INTO msgs_broadcast_contacts(broadcast_id, contact_id) VALUES($1, $2)`, id, contact.ID)
	}
	for _, group := range groups {
		rt.DB.MustExec(`INSERT INTO msgs_broadcast_groups(broadcast_id, contactgroup_id) VALUES($1, $2)`, id, group.ID)
	}

	return id
}

// InsertOptIn inserts an opt in
func InsertOptIn(rt *runtime.Runtime, org *Org, name string) *OptIn {
	uuid := assets.OptInUUID(uuids.New())
	var id models.OptInID
	must(rt.DB.Get(&id,
		`INSERT INTO msgs_optin(uuid, org_id, name, created_on, modified_on, created_by_id, modified_by_id, is_active, is_system) 
		VALUES($1, $2, $3, NOW(), NOW(), 1, 1, TRUE, FALSE) RETURNING id`, uuid, org.ID, name,
	))
	return &OptIn{ID: id, UUID: uuid}
}
