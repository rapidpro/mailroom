package models

import (
	"context"
	"encoding/json"
	"net/url"
	"strconv"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/juju/errors"
	"github.com/lib/pq"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/sirupsen/logrus"
	null "gopkg.in/guregu/null.v3"
)

type MsgDirection string

const (
	DirectionIn  = MsgDirection("I")
	DirectionOut = MsgDirection("O")
)

type MsgVisibility string

const (
	VisibilityVisible  = MsgVisibility("V")
	VisibilityArchived = MsgVisibility("A")
	VisibilityDeleted  = MsgVisibility("D")
)

type MsgType string

const (
	TypeInbox = MsgType("I")
	TypeFlow  = MsgType("F")
	TypeIVR   = MsgType("V")
	TypeUSSD  = MsgType("U")
)

type ConnectionID null.Int

type MsgStatus string

const (
	MsgStatusInitializing = MsgStatus("I")
	MsgStatusPending      = MsgStatus("P")
	MsgStatusQueued       = MsgStatus("Q")
	MsgStatusWired        = MsgStatus("W")
	MsgStatusSent         = MsgStatus("S")
	MsgStatusHandled      = MsgStatus("H")
	MsgStatusErrored      = MsgStatus("E")
	MsgStatusFailed       = MsgStatus("F")
	MsgStatusResent       = MsgStatus("R")
)

// TODO: response_to_id, response_to_external_id

// Msg is our type for mailroom messages
type Msg struct {
	m struct {
		ID           flows.MsgID        `db:"id"              json:"id"`
		UUID         flows.MsgUUID      `db:"uuid"            json:"uuid"`
		Text         string             `db:"text"            json:"text"`
		HighPriority bool               `db:"high_priority"   json:"high_priority"`
		CreatedOn    time.Time          `db:"created_on"      json:"created_on"`
		ModifiedOn   time.Time          `db:"modified_on"     json:"modified_on"`
		SentOn       time.Time          `db:"sent_on"         json:"sent_on"`
		QueuedOn     time.Time          `db:"queued_on"       json:"queued_on"`
		Direction    MsgDirection       `db:"direction"       json:"direction"`
		Status       MsgStatus          `db:"status"          json:"status"`
		Visibility   MsgVisibility      `db:"visibility"      json:"visibility"`
		MsgType      MsgType            `db:"msg_type"`
		MsgCount     int                `db:"msg_count"       json:"tps_cost"`
		ErrorCount   int                `db:"error_count"     json:"error_count"`
		NextAttempt  time.Time          `db:"next_attempt"    json:"next_attempt"`
		ExternalID   null.String        `db:"external_id"     json:"external_id"`
		Attachments  pq.StringArray     `db:"attachments"     json:"attachments"`
		Metadata     null.String        `db:"metadata"        json:"metadata"`
		ChannelID    ChannelID          `db:"channel_id"      json:"channel_id"`
		ChannelUUID  assets.ChannelUUID `                     json:"channel_uuid"`
		ConnectionID ConnectionID       `db:"connection_id"`
		ContactID    flows.ContactID    `db:"contact_id"      json:"contact_id"`
		ContactURNID URNID              `db:"contact_urn_id"  json:"contact_urn_id"`
		URN          urns.URN           `                     json:"urn"`
		URNAuth      string             `                     json:"urn_auth,omitempty"`
		OrgID        OrgID              `db:"org_id"          json:"org_id"`
		TopupID      TopupID            `db:"topup_id"`
	}

	channel *Channel
}

func (m *Msg) ID() flows.MsgID                 { return m.m.ID }
func (m *Msg) UUID() flows.MsgUUID             { return m.m.UUID }
func (m *Msg) Channel() *Channel               { return m.channel }
func (m *Msg) Text() string                    { return m.m.Text }
func (m *Msg) HighPriority() bool              { return m.m.HighPriority }
func (m *Msg) CreatedOn() time.Time            { return m.m.CreatedOn }
func (m *Msg) ModifiedOn() time.Time           { return m.m.ModifiedOn }
func (m *Msg) SentOn() time.Time               { return m.m.SentOn }
func (m *Msg) QueuedOn() time.Time             { return m.m.QueuedOn }
func (m *Msg) Direction() MsgDirection         { return m.m.Direction }
func (m *Msg) Status() MsgStatus               { return m.m.Status }
func (m *Msg) Visibility() MsgVisibility       { return m.m.Visibility }
func (m *Msg) MsgType() MsgType                { return m.m.MsgType }
func (m *Msg) ErrorCount() int                 { return m.m.ErrorCount }
func (m *Msg) NextAttempt() time.Time          { return m.m.NextAttempt }
func (m *Msg) ExternalID() null.String         { return m.m.ExternalID }
func (m *Msg) Metadata() null.String           { return m.m.Metadata }
func (m *Msg) ChannelID() ChannelID            { return m.m.ChannelID }
func (m *Msg) ChannelUUID() assets.ChannelUUID { return m.m.ChannelUUID }
func (m *Msg) ConnectionID() ConnectionID      { return m.m.ConnectionID }
func (m *Msg) URN() urns.URN                   { return m.m.URN }
func (m *Msg) URNAuth() string                 { return m.m.URNAuth }
func (m *Msg) OrgID() OrgID                    { return m.m.OrgID }
func (m *Msg) TopupID() TopupID                { return m.m.TopupID }
func (m *Msg) ContactID() flows.ContactID      { return m.m.ContactID }
func (m *Msg) ContactURNID() URNID             { return m.m.ContactURNID }
func (m *Msg) SetTopup(topupID TopupID)        { m.m.TopupID = topupID }

func (m *Msg) Attachments() []flows.Attachment {
	attachments := make([]flows.Attachment, len(m.m.Attachments))
	for i := range m.m.Attachments {
		attachments[i] = flows.Attachment(m.m.Attachments[i])
	}
	return attachments
}

// NewOutgoingMsg creates an outgoing message for the passed in flow message. Note that this message is created in a queued state!
func NewOutgoingMsg(orgID OrgID, channel *Channel, contactID flows.ContactID, out *flows.MsgOut, createdOn time.Time) (*Msg, error) {
	_, _, query, _ := out.URN().ToParts()
	parsedQuery, err := url.ParseQuery(query)
	if err != nil {
		return nil, errors.Annotatef(err, "unable to parse urn: %s", out.URN())
	}

	// get the id of our URN
	idQuery := parsedQuery.Get("id")
	urnID, err := strconv.Atoi(idQuery)
	if urnID == 0 {
		return nil, errors.Annotatef(err, "unable to create msg for URN, has no id: %s", out.URN())
	}

	msg := &Msg{}
	m := &msg.m
	m.UUID = out.UUID()
	m.Text = out.Text()
	m.HighPriority = true
	m.Direction = DirectionOut
	m.Status = MsgStatusQueued
	m.Visibility = VisibilityVisible
	m.MsgType = TypeFlow
	m.ContactID = contactID
	m.ContactURNID = URNID(urnID)
	m.URN = out.URN()
	m.OrgID = orgID
	m.TopupID = NilTopupID
	m.CreatedOn = createdOn
	m.ChannelID = channel.ID()
	m.ChannelUUID = channel.UUID()

	msg.channel = channel

	// if we have attachments, add them
	if len(out.Attachments()) > 0 {
		for _, a := range out.Attachments() {
			m.Attachments = append(m.Attachments, string(a))
		}
	}

	// if we have quick replies, populate our metadata
	if len(out.QuickReplies()) > 0 {
		metadata := make(map[string]interface{})
		metadata["quick_replies"] = out.QuickReplies()

		metadataJSON, err := json.Marshal(metadata)
		if err != nil {
			return nil, errors.Annotate(err, "error marshalling quick replies")
		}
		m.Metadata.SetValid(string(metadataJSON))
	}

	// set URN auth info if we have any (this is used when queuing later on)
	urnAuth := parsedQuery.Get("auth")
	if urnAuth != "" {
		m.URNAuth = urnAuth
	}

	// TODO: calculate real msg count
	return msg, nil
}

// InsertMessages inserts the passed in messages in a single query
func InsertMessages(ctx context.Context, tx Queryer, msgs []*Msg) error {
	is := make([]interface{}, len(msgs))
	for i := range msgs {
		is[i] = &msgs[i].m
		logrus.WithField("msg_uuid", msgs[i].UUID()).Info("inserting message")
	}

	return BulkSQL(ctx, "insert messages", tx, insertMsgSQL, is)
}

const insertMsgSQL = `
INSERT INTO
msgs_msg(uuid, text, high_priority, created_on, modified_on, queued_on, direction, status, attachments, metadata,
		 visibility, msg_type, msg_count, error_count, next_attempt, channel_id, 
		 contact_id, contact_urn_id, org_id, topup_id)
  VALUES(:uuid, :text, :high_priority, :created_on, now(), now(), :direction, :status, :attachments, :metadata,
		 :visibility, :msg_type, :msg_count, :error_count, :next_attempt, :channel_id, 
		 :contact_id, :contact_urn_id, :org_id, :topup_id)
RETURNING 
	id as id, 
	now() as modified_on,
	now() as queued_on
`

// UpdateMessage updates the passed in message status, visibility and msg type
func UpdateMessage(ctx context.Context, tx Queryer, msgID flows.MsgID, status MsgStatus, visibility MsgVisibility, msgType MsgType, topup TopupID) error {
	_, err := tx.QueryxContext(
		ctx,
		`UPDATE 
			msgs_msg 
		SET 
			status = $2,
			visibility = $3,
			msg_type = $4,
			topup_id = $5
		WHERE
			id = $1`,
		msgID, status, visibility, msgType, topup)

	if err != nil {
		return errors.Annotatef(err, "error updating msg: %d", msgID)
	}

	return nil
}

// MarkMessagesPending marks the passed in messages as pending
func MarkMessagesPending(ctx context.Context, tx *sqlx.Tx, msgs []*Msg) error {
	return updateMessageStatus(ctx, tx, msgs, MsgStatusPending)
}

// MarkMessagesQueued marks the passed in messages as queued
func MarkMessagesQueued(ctx context.Context, tx *sqlx.Tx, msgs []*Msg) error {
	return updateMessageStatus(ctx, tx, msgs, MsgStatusQueued)
}

// MarkMessagesQueued marks the passed in messages as queued
func updateMessageStatus(ctx context.Context, tx *sqlx.Tx, msgs []*Msg, status MsgStatus) error {
	ids := make([]int, len(msgs))
	for i, m := range msgs {
		ids[i] = int(m.m.ID)
	}

	q, vs, err := sqlx.In(updateMsgStatusSQL, ids, status)
	if err != nil {
		return errors.Annotate(err, "error preparing query for updating message status")
	}
	q = tx.Rebind(q)

	_, err = tx.ExecContext(ctx, q, vs...)
	if err != nil {
		return errors.Annotate(err, "error updating message status")
	}

	return nil
}

const updateMsgStatusSQL = `
UPDATE 
	msgs_msg
SET 
	status = $1, 
	modified_on = NOW()
WHERE
	id IN (?)
`
