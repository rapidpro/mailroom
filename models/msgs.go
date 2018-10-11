package models

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/jmoiron/sqlx/types"
	"github.com/juju/errors"
	"github.com/lib/pq"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/gsm7"
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
	Metadata     types.JSONText     `db:"metadata"        json:"metadata"`
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

// NewOutgoingMsg creates an outgoing message for the passed in flow message. Note
// that this message is created in a queued state!
func NewOutgoingMsg(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, orgID OrgID, channel *Channel, contactID flows.ContactID, m *flows.MsgOut, createdOn time.Time) (*Msg, error) {
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
		CreatedOn:    createdOn,

		channel:     channel,
		ChannelID:   channel.ID(),
		ChannelUUID: channel.UUID(),
	}

	// if we have attachments, add them
	if len(m.Attachments()) > 0 {
		for _, a := range m.Attachments() {
			// if our URL is relative, remap it to something fully qualified
			url := a.URL()
			if !strings.HasPrefix(url, "http") {
				if strings.HasPrefix(url, "/") {
					url = fmt.Sprintf("https://%s%s", mailroom.Config.AttachmentDomain, url)
				} else {
					url = fmt.Sprintf("https://%s/%s", mailroom.Config.AttachmentDomain, url)
				}
			}
			msg.Attachments = append(msg.Attachments, fmt.Sprintf("%s:%s", a.ContentType(), url))
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
		msg.Metadata = metadataJSON
	}

	// set URN auth info if we have any (this is used when queuing later on)
	urnAuth := parsedQuery.Get("auth")
	if urnAuth != "" {
		msg.URNAuth = urnAuth
	}

	// calculate msg count
	if m.URN().Scheme() == urns.TelScheme {
		msg.MsgCount = gsm7.Segments(m.Text()) + len(m.Attachments())
	} else {
		msg.MsgCount = 1
	}

	return msg, nil
}

const InsertMsgSQL = `
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
		ids[i] = int(m.ID)
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
