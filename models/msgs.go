package models

import (
	"context"
	"encoding/json"
	"net/url"
	"strconv"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/juju/errors"
	"github.com/lib/pq"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
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
type ContactURNID int

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
	ContactURNID ContactURNID       `db:"contact_urn_id"  json:"contact_urn_id"`
	URN          urns.URN           `                     json:"urn"`
	URNAuth      string             `                     json:"urn_auth,omitempty"`
	OrgID        OrgID              `db:"org_id"          json:"org_id"`
	TopUpID      TopupID            `db:"topup_id"`

	channel *Channel
}

// Channel returns the db channel object for this channel
func (m *Msg) Channel() *Channel { return m.channel }

// newOutgoingMsg creates an outgoing message for the passed in flow message. Note
// that this message is created in a queued state!
func newOutgoingMsg(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, orgID OrgID, channel *Channel, contactID flows.ContactID, m *flows.MsgOut) (*Msg, error) {
	_, _, query, _ := m.URN().ToParts()
	parsedQuery, err := url.ParseQuery(query)
	if err != nil {
		return nil, errors.Annotatef(err, "unable to parse urn: %s", m.URN())
	}

	// get the id of our URN
	idQuery := parsedQuery.Get("id")
	urnID, err := strconv.Atoi(idQuery)
	if urnID == 0 {
		return nil, errors.Annotatef(err, "unable to create msg for URN, has no id: %s", m.URN())
	}

	msg := &Msg{
		UUID:         m.UUID(),
		Text:         m.Text(),
		HighPriority: true,
		Direction:    DirectionOut,
		Status:       MsgStatusQueued,
		Visibility:   VisibilityVisible,
		MsgType:      TypeFlow,
		ContactID:    contactID,
		ContactURNID: ContactURNID(urnID),
		URN:          m.URN(),
		OrgID:        orgID,
		TopUpID:      NilTopupID,

		channel:     channel,
		ChannelID:   channel.ID(),
		ChannelUUID: channel.UUID(),
	}

	// if we have attachments, add them
	if len(m.Attachments()) > 0 {
		for _, a := range m.Attachments() {
			msg.Attachments = append(msg.Attachments, string(a))
		}
	}

	// if we have quick replies, populate our metadata
	if len(m.QuickReplies()) > 0 {
		metadata := make(map[string]interface{})
		metadata["quick_replies"] = m.QuickReplies()

		metadataJSON, err := json.Marshal(metadata)
		if err != nil {
			return nil, errors.Annotate(err, "error marshalling quick replies")
		}
		msg.Metadata.SetValid(string(metadataJSON))
	}

	// set URN auth info if we have any (this is used when queuing later on)
	urnAuth := parsedQuery.Get("auth")
	if urnAuth != "" {
		msg.URNAuth = urnAuth
	}

	// TODO: calculate real msg count
	return msg, nil
}

const insertMsgSQL = `
INSERT INTO
msgs_msg(uuid, text, high_priority, created_on, modified_on, queued_on, direction, status, attachments, metadata,
		 visibility, msg_type, msg_count, error_count, next_attempt, channel_id, 
		 contact_id, contact_urn_id, org_id, topup_id)
  VALUES(:uuid, :text, :high_priority, now(), now(), now(), :direction, :status, :attachments, :metadata,
		 :visibility, :msg_type, :msg_count, :error_count, :next_attempt, :channel_id, 
		 :contact_id, :contact_urn_id, :org_id, :topup_id)
RETURNING 
	id as id, 
	now() as created_on,
	now() as modified_on,
	now() as queued_on
`

// insertSessionMessages takes care of inserting all the messages in the passed in sessions assigning topups
// to them as needed.
func insertSessionMessages(ctx context.Context, tx *sqlx.Tx, rc redis.Conn, orgID OrgID, sessions []*Session) error {
	// build all the messages that need inserting
	msgs := make([]interface{}, 0, len(sessions))
	for _, s := range sessions {
		for _, m := range s.Outbox() {
			msgs = append(msgs, m)
		}
	}

	// find the topup we will assign
	topup, err := decrementOrgCredits(ctx, tx, rc, orgID, len(msgs))
	if err != nil {
		return errors.Annotatef(err, "error finding active topup")
	}

	// if we have an active topup, assign it to our messages
	if topup != NilTopupID {
		for _, m := range msgs {
			m.(*Msg).TopUpID = topup
		}
	}

	// insert all our messages
	err = bulkInsert(ctx, tx, insertMsgSQL, msgs)
	if err != nil {
		return errors.Annotatef(err, "error writing messages")
	}

	return nil
}

// MarkMessagesPending marks the passed in messages as pending
func MarkMessagesPending(ctx context.Context, db *sqlx.DB, msgs []*Msg) error {
	return updateMessageStatus(ctx, db, msgs, MsgStatusPending)
}

// MarkMessagesQueued marks the passed in messages as queued
func MarkMessagesQueued(ctx context.Context, db *sqlx.DB, msgs []*Msg) error {
	return updateMessageStatus(ctx, db, msgs, MsgStatusQueued)
}

// MarkMessagesQueued marks the passed in messages as queued
func updateMessageStatus(ctx context.Context, db *sqlx.DB, msgs []*Msg, status MsgStatus) error {
	ids := make([]int, len(msgs))
	for i, m := range msgs {
		ids[i] = int(m.ID)
	}

	q, vs, err := sqlx.In(updateMsgStatusSQL, ids, status)
	if err != nil {
		return errors.Annotate(err, "error preparing query for updating message status")
	}
	q = db.Rebind(q)

	_, err = db.ExecContext(ctx, q, vs...)
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
