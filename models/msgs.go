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
type TopUpID null.Int

type MsgStatus string

const (
	StatusInitializing = MsgStatus("I")
	StatusPending      = MsgStatus("P")
	StatusQueued       = MsgStatus("Q")
	StatusWired        = MsgStatus("W")
	StatusSent         = MsgStatus("S")
	StatusHandled      = MsgStatus("H")
	StatusErrored      = MsgStatus("E")
	StatusFailed       = MsgStatus("F")
	StatusResent       = MsgStatus("R")
)

// TODO: response_to_id, response_to_external_id
// TODO: real tps_cost

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
	TopUpID      TopUpID            `db:"topup_id"`
}

// CreateOutgoingMsg creates an outgoing message for the passed in flow message
func CreateOutgoingMsg(ctx context.Context, tx *sqlx.Tx, orgID OrgID, channelID ChannelID, contactID flows.ContactID, m *flows.MsgOut) (*Msg, error) {
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

	// get the id of our active topup
	topupID, err := loadActiveTopup(ctx, tx, orgID)
	if err != nil {
		return nil, errors.Annotatef(err, "error getting active topup for msg")
	}

	// for now it is an error to try to create a msg without a channel
	if m.Channel() == nil {
		return nil, errors.Errorf("attempt to create a msg without a channel")
	}

	msg := &Msg{
		UUID:         m.UUID(),
		Text:         m.Text(),
		HighPriority: true,
		Direction:    DirectionOut,
		Status:       StatusPending,
		Visibility:   VisibilityVisible,
		MsgType:      TypeFlow,
		ContactID:    contactID,
		ContactURNID: ContactURNID(urnID),
		URN:          m.URN(),
		OrgID:        orgID,
		TopUpID:      topupID,
		ChannelID:    channelID,
		ChannelUUID:  m.Channel().UUID,
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

	// insert msg
	rows, err := tx.NamedQuery(insertMsgSQL, msg)
	if err != nil {
		return nil, errors.Annotate(err, "error inserting new outgoing message")
	}
	rows.Next()
	var insertTime time.Time
	err = rows.Scan(&msg.ID, &insertTime)
	if err != nil {
		return nil, errors.Annotate(err, "error scanning msg id during creation")
	}
	rows.Close()

	// populate our insert time
	msg.CreatedOn = insertTime
	msg.ModifiedOn = insertTime

	// return it
	return msg, nil
}

const insertMsgSQL = `
INSERT INTO
msgs_msg(uuid, text, high_priority, created_on, modified_on, direction, status, attachments, metadata,
		 visibility, msg_type, msg_count, error_count, next_attempt, channel_id, 
		 contact_id, contact_urn_id, org_id, topup_id)
  VALUES(:uuid, :text, :high_priority, NOW(), NOW(), :direction, :status, :attachments, :metadata,
		 :visibility, :msg_type, :msg_count, :error_count, :next_attempt, :channel_id, 
		 :contact_id, :contact_urn_id, :org_id, :topup_id)
RETURNING id, NOW()
`

// MarkMessagesQueued marks the passed in messages as queued
func MarkMessagesQueued(ctx context.Context, db *sqlx.DB, msgs []*Msg) error {
	ids := make([]int, len(msgs))
	for i, m := range msgs {
		ids[i] = int(m.ID)
	}

	q, vs, err := sqlx.In(queueMsgSQL, ids)
	if err != nil {
		return errors.Annotate(err, "error preparing query for queuing messages")
	}
	q = db.Rebind(q)

	// TODO: use real queued on instead of now()
	_, err = db.ExecContext(ctx, q, vs...)
	if err != nil {
		return errors.Annotate(err, "error marking message as queued")
	}

	return nil
}

const queueMsgSQL = `
UPDATE 
	msgs_msg
SET 
	status = 'Q', 
	queued_on = NOW(),
	modified_on = NOW()
WHERE
	id IN (?)
`
