package models

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/lib/pq"
	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/gocommon/gsm7"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/null/v2"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// maximum number of repeated messages to same contact allowed in 5 minute window
const msgRepetitionLimit = 20

// MsgID is our internal type for msg ids, which can be null/0
type MsgID int64

// NilMsgID is our constant for a nil msg id
const NilMsgID = MsgID(0)

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
	MsgTypeText  = MsgType("T")
	MsgTypeVoice = MsgType("V")
)

type MsgStatus string

const (
	MsgStatusPending      = MsgStatus("P") // incoming msg created but not yet handled
	MsgStatusHandled      = MsgStatus("H") // incoming msg handled
	MsgStatusInitializing = MsgStatus("I") // outgoing message that failed to queue
	MsgStatusQueued       = MsgStatus("Q") // outgoing msg created and queued to courier
	MsgStatusWired        = MsgStatus("W") // outgoing msg requested to be sent via channel
	MsgStatusSent         = MsgStatus("S") // outgoing msg having received sent confirmation from channel
	MsgStatusDelivered    = MsgStatus("D") // outgoing msg having received delivery confirmation from channel
	MsgStatusErrored      = MsgStatus("E") // outgoing msg which has errored and will be retried
	MsgStatusFailed       = MsgStatus("F") // outgoing msg which has failed permanently
)

type MsgFailedReason null.String

const (
	NilMsgFailedReason      = MsgFailedReason("")
	MsgFailedSuspended      = MsgFailedReason("S") // workspace suspended
	MsgFailedContact        = MsgFailedReason("C") // contact blocked, stopped or archived
	MsgFailedLooping        = MsgFailedReason("L")
	MsgFailedErrorLimit     = MsgFailedReason("E")
	MsgFailedTooOld         = MsgFailedReason("O")
	MsgFailedNoDestination  = MsgFailedReason("D")
	MsgFailedChannelRemoved = MsgFailedReason("R")
)

var unsendableToFailedReason = map[flows.UnsendableReason]MsgFailedReason{
	flows.UnsendableReasonContactStatus: MsgFailedContact,
	flows.UnsendableReasonNoDestination: MsgFailedNoDestination,
}

// Msg is our type for mailroom messages
type Msg struct {
	m struct {
		ID    flows.MsgID   `db:"id"`
		UUID  flows.MsgUUID `db:"uuid"`
		OrgID OrgID         `db:"org_id"`

		// origin
		BroadcastID BroadcastID `db:"broadcast_id"`
		FlowID      FlowID      `db:"flow_id"`
		CreatedByID UserID      `db:"created_by_id"`

		// content
		Text         string         `db:"text"`
		Attachments  pq.StringArray `db:"attachments"`
		QuickReplies pq.StringArray `db:"quick_replies"`
		Locale       envs.Locale    `db:"locale"`

		HighPriority bool          `db:"high_priority"`
		Direction    MsgDirection  `db:"direction"`
		Status       MsgStatus     `db:"status"`
		Visibility   MsgVisibility `db:"visibility"`
		MsgType      MsgType       `db:"msg_type"`
		MsgCount     int           `db:"msg_count"`
		CreatedOn    time.Time     `db:"created_on"`
		ModifiedOn   time.Time     `db:"modified_on"`
		ExternalID   null.String   `db:"external_id"`
		Metadata     null.Map      `db:"metadata"`
		ChannelID    ChannelID     `db:"channel_id"`
		ContactID    ContactID     `db:"contact_id"`
		ContactURNID *URNID        `db:"contact_urn_id"`
		URN          urns.URN      `db:"urn_urn"`
		URNAuth      null.String   `db:"urn_auth"`

		SentOn       *time.Time      `db:"sent_on"`
		QueuedOn     time.Time       `db:"queued_on"`
		ErrorCount   int             `db:"error_count"`
		NextAttempt  *time.Time      `db:"next_attempt"`
		FailedReason MsgFailedReason `db:"failed_reason"`
	}

	// extra data added to the courier payload
	Flow                 *assets.FlowReference `json:"flow,omitempty"`
	ResponseToExternalID null.String           `json:"response_to_external_id,omitempty"`
	IsResend             bool                  `json:"is_resend,omitempty"`
	SessionID            SessionID             `json:"session_id,omitempty"`
	SessionStatus        SessionStatus         `json:"session_status,omitempty"`

	// These fields are set on the last outgoing message in a session's sprint. In the case
	// of the session being at a wait with a timeout then the timeout will be set. It is up to
	// Courier to update the session's timeout appropriately after sending the message.
	SessionWaitStartedOn *time.Time `json:"session_wait_started_on,omitempty"`
	SessionTimeout       int        `json:"session_timeout,omitempty"`
}

func (m *Msg) ID() flows.MsgID                  { return m.m.ID }
func (m *Msg) BroadcastID() BroadcastID         { return m.m.BroadcastID }
func (m *Msg) UUID() flows.MsgUUID              { return m.m.UUID }
func (m *Msg) Text() string                     { return m.m.Text }
func (m *Msg) QuickReplies() []string           { return m.m.QuickReplies }
func (m *Msg) Locale() envs.Locale              { return m.m.Locale }
func (m *Msg) HighPriority() bool               { return m.m.HighPriority }
func (m *Msg) CreatedOn() time.Time             { return m.m.CreatedOn }
func (m *Msg) ModifiedOn() time.Time            { return m.m.ModifiedOn }
func (m *Msg) SentOn() *time.Time               { return m.m.SentOn }
func (m *Msg) QueuedOn() time.Time              { return m.m.QueuedOn }
func (m *Msg) Direction() MsgDirection          { return m.m.Direction }
func (m *Msg) Status() MsgStatus                { return m.m.Status }
func (m *Msg) Visibility() MsgVisibility        { return m.m.Visibility }
func (m *Msg) Type() MsgType                    { return m.m.MsgType }
func (m *Msg) ErrorCount() int                  { return m.m.ErrorCount }
func (m *Msg) NextAttempt() *time.Time          { return m.m.NextAttempt }
func (m *Msg) FailedReason() MsgFailedReason    { return m.m.FailedReason }
func (m *Msg) ExternalID() null.String          { return m.m.ExternalID }
func (m *Msg) Metadata() map[string]interface{} { return m.m.Metadata }
func (m *Msg) MsgCount() int                    { return m.m.MsgCount }
func (m *Msg) ChannelID() ChannelID             { return m.m.ChannelID }
func (m *Msg) URN() urns.URN                    { return m.m.URN }
func (m *Msg) URNAuth() null.String             { return m.m.URNAuth }
func (m *Msg) OrgID() OrgID                     { return m.m.OrgID }
func (m *Msg) FlowID() FlowID                   { return m.m.FlowID }
func (m *Msg) ContactID() ContactID             { return m.m.ContactID }
func (m *Msg) ContactURNID() *URNID             { return m.m.ContactURNID }

func (m *Msg) SetChannel(channel *Channel) {
	if channel != nil {
		m.m.ChannelID = channel.ID()
	} else {
		m.m.ChannelID = NilChannelID
	}
}

func (m *Msg) SetURN(urn urns.URN) error {
	// noop for nil urn
	if urn == urns.NilURN {
		return nil
	}

	m.m.URN = urn

	// set our ID if we have one
	urnInt := GetURNInt(urn, "id")
	if urnInt == 0 {
		return errors.Errorf("missing urn id on urn: %s", urn)
	}

	urnID := URNID(urnInt)
	m.m.ContactURNID = &urnID
	m.m.URNAuth = GetURNAuth(urn)

	return nil
}

func (m *Msg) Attachments() []utils.Attachment {
	attachments := make([]utils.Attachment, len(m.m.Attachments))
	for i := range m.m.Attachments {
		attachments[i] = utils.Attachment(m.m.Attachments[i])
	}
	return attachments
}

func (m *Msg) MarshalJSON() ([]byte, error) {
	return json.Marshal(m.m)
}

// NewIncomingIVR creates a new incoming IVR message for the passed in text and attachment
func NewIncomingIVR(cfg *runtime.Config, orgID OrgID, call *Call, in *flows.MsgIn, createdOn time.Time) *Msg {
	msg := &Msg{}
	m := &msg.m

	msg.SetURN(in.URN())
	m.UUID = in.UUID()
	m.Text = in.Text()
	m.Direction = DirectionIn
	m.Status = MsgStatusHandled
	m.Visibility = VisibilityVisible
	m.MsgType = MsgTypeVoice
	m.ContactID = call.ContactID()

	urnID := call.ContactURNID()
	m.ContactURNID = &urnID
	m.ChannelID = call.ChannelID()

	m.OrgID = orgID
	m.CreatedOn = createdOn

	// add any attachments
	for _, a := range in.Attachments() {
		m.Attachments = append(m.Attachments, string(NormalizeAttachment(cfg, a)))
	}

	return msg
}

// NewOutgoingIVR creates a new IVR message for the passed in text with the optional attachment
func NewOutgoingIVR(cfg *runtime.Config, orgID OrgID, call *Call, out *flows.MsgOut, createdOn time.Time) *Msg {
	msg := &Msg{}
	m := &msg.m

	msg.SetURN(out.URN())
	m.UUID = out.UUID()
	m.Text = out.Text()
	m.Locale = out.Locale()
	m.HighPriority = false
	m.Direction = DirectionOut
	m.Status = MsgStatusWired
	m.Visibility = VisibilityVisible
	m.MsgType = MsgTypeVoice
	m.ContactID = call.ContactID()

	urnID := call.ContactURNID()
	m.ContactURNID = &urnID
	m.ChannelID = call.ChannelID()

	m.URN = out.URN()

	m.OrgID = orgID
	m.CreatedOn = createdOn
	m.SentOn = &createdOn

	// if we have attachments, add them
	for _, a := range out.Attachments() {
		m.Attachments = append(m.Attachments, string(NormalizeAttachment(cfg, a)))
	}

	return msg
}

// NewOutgoingFlowMsg creates an outgoing message for the passed in flow message
func NewOutgoingFlowMsg(rt *runtime.Runtime, org *Org, channel *Channel, session *Session, flow *Flow, out *flows.MsgOut, createdOn time.Time) (*Msg, error) {
	return newOutgoingTextMsg(rt, org, channel, session.Contact(), out, createdOn, session, flow, NilBroadcastID, NilUserID)
}

// NewOutgoingBroadcastMsg creates an outgoing message which is part of a broadcast
func NewOutgoingBroadcastMsg(rt *runtime.Runtime, org *Org, channel *Channel, contact *flows.Contact, out *flows.MsgOut, createdOn time.Time, bb *BroadcastBatch) (*Msg, error) {
	return newOutgoingTextMsg(rt, org, channel, contact, out, createdOn, nil, nil, bb.BroadcastID, bb.CreatedByID)
}

// NewOutgoingChatMsg creates an outgoing message from chat
func NewOutgoingChatMsg(rt *runtime.Runtime, org *Org, channel *Channel, contact *flows.Contact, out *flows.MsgOut, createdOn time.Time, userID UserID) (*Msg, error) {
	return newOutgoingTextMsg(rt, org, channel, contact, out, createdOn, nil, nil, NilBroadcastID, userID)
}

func newOutgoingTextMsg(rt *runtime.Runtime, org *Org, channel *Channel, contact *flows.Contact, out *flows.MsgOut, createdOn time.Time, session *Session, flow *Flow, broadcastID BroadcastID, userID UserID) (*Msg, error) {
	msg := &Msg{}
	m := &msg.m
	m.UUID = out.UUID()
	m.OrgID = org.ID()
	m.ContactID = ContactID(contact.ID())
	m.BroadcastID = broadcastID
	m.Text = out.Text()
	m.QuickReplies = out.QuickReplies()
	m.Locale = out.Locale()
	m.HighPriority = false
	m.Direction = DirectionOut
	m.Status = MsgStatusQueued
	m.Visibility = VisibilityVisible
	m.MsgType = MsgTypeText
	m.MsgCount = 1
	m.CreatedOn = createdOn
	m.CreatedByID = userID
	m.Metadata = null.Map(buildMsgMetadata(out))

	msg.SetChannel(channel)
	msg.SetURN(out.URN())

	// if we have attachments, add them
	if len(out.Attachments()) > 0 {
		for _, a := range out.Attachments() {
			m.Attachments = append(m.Attachments, string(NormalizeAttachment(rt.Config, a)))
		}
	}

	if out.UnsendableReason() != flows.NilUnsendableReason {
		m.Status = MsgStatusFailed
		m.FailedReason = unsendableToFailedReason[out.UnsendableReason()]
	} else if org.Suspended() {
		// we fail messages for suspended orgs right away
		m.Status = MsgStatusFailed
		m.FailedReason = MsgFailedSuspended
	} else {
		// also fail right away if this looks like a loop
		repetitions, err := GetMsgRepetitions(rt.RP, contact, out)
		if err != nil {
			return nil, errors.Wrap(err, "error looking up msg repetitions")
		}
		if repetitions >= msgRepetitionLimit {
			m.Status = MsgStatusFailed
			m.FailedReason = MsgFailedLooping

			logrus.WithFields(logrus.Fields{"contact_id": contact.ID(), "text": out.Text(), "repetitions": repetitions}).Error("too many repetitions, failing message")
		}
	}

	// if we have a session, set fields on the message from that
	if session != nil {
		msg.ResponseToExternalID = session.IncomingMsgExternalID()
		msg.SessionID = session.ID()
		msg.SessionStatus = session.Status()

		if flow != nil {
			m.FlowID = flow.ID()
			msg.Flow = flow.Reference()
		}

		// if we're responding to an incoming message, send as high priority
		if session.IncomingMsgID() != NilMsgID {
			m.HighPriority = true
		}
	}

	// if we're sending to a phone, message may have to be sent in multiple parts
	if m.URN.Scheme() == urns.TelScheme {
		m.MsgCount = gsm7.Segments(m.Text) + len(m.Attachments)
	}

	return msg, nil
}

func buildMsgMetadata(m *flows.MsgOut) map[string]interface{} {
	metadata := make(map[string]interface{})
	if m.Templating() != nil {
		metadata["templating"] = m.Templating()
	}
	if m.Topic() != flows.NilMsgTopic {
		metadata["topic"] = string(m.Topic())
	}
	return metadata
}

// NewIncomingSurveyorMsg creates a new incoming message for the passed in text and attachment
func NewIncomingSurveyorMsg(cfg *runtime.Config, orgID OrgID, channel *Channel, contactID ContactID, in *flows.MsgIn, createdOn time.Time) *Msg {
	msg := &Msg{}

	msg.SetChannel(channel)
	msg.SetURN(in.URN())

	m := &msg.m
	m.UUID = in.UUID()
	m.Text = in.Text()
	m.Direction = DirectionIn
	m.Status = MsgStatusHandled
	m.Visibility = VisibilityVisible
	m.MsgType = MsgTypeText
	m.ContactID = contactID
	m.OrgID = orgID
	m.CreatedOn = createdOn

	// add any attachments
	for _, a := range in.Attachments() {
		m.Attachments = append(m.Attachments, string(NormalizeAttachment(cfg, a)))
	}

	return msg
}

var msgRepetitionsScript = redis.NewScript(3, `
local key, contact_id, text = KEYS[1], KEYS[2], KEYS[3]

local msg_key = string.format("%d|%s", contact_id, string.lower(string.sub(text, 1, 128)))
local count = 1

-- try to look up in window
local record = redis.call("HGET", key, msg_key)
if record then
	count = tonumber(record) + 1
end

-- write updated count and set expiration
redis.call("HSET", key, msg_key, count)
redis.call("EXPIRE", key, 300)

return count
`)

// GetMsgRepetitions gets the number of repetitions of this msg text for the given contact in the current 5 minute window
func GetMsgRepetitions(rp *redis.Pool, contact *flows.Contact, msg *flows.MsgOut) (int, error) {
	rc := rp.Get()
	defer rc.Close()

	keyTime := dates.Now().UTC().Round(time.Minute * 5)
	key := fmt.Sprintf("msg_repetitions:%s", keyTime.Format("2006-01-02T15:04"))
	return redis.Int(msgRepetitionsScript.Do(rc, key, contact.ID(), msg.Text()))
}

var loadMessagesSQL = `
SELECT 
	id,
	broadcast_id,
	uuid,
	text,
	attachments,
	quick_replies,
	locale,
	created_on,
	direction,
	status,
	visibility,
	msg_count,
	error_count,
	next_attempt,
	failed_reason,
	coalesce(high_priority, FALSE) as high_priority,
	external_id,
	metadata,
	channel_id,
	contact_id,
	contact_urn_id,
	org_id
FROM
	msgs_msg
WHERE
	org_id = $1 AND
	direction = $2 AND
	id = ANY($3)
ORDER BY
	id ASC`

// GetMessagesByID fetches the messages with the given ids
func GetMessagesByID(ctx context.Context, db Queryer, orgID OrgID, direction MsgDirection, msgIDs []MsgID) ([]*Msg, error) {
	return loadMessages(ctx, db, loadMessagesSQL, orgID, direction, pq.Array(msgIDs))
}

var loadMessagesForRetrySQL = `
SELECT 
	m.id,
	m.broadcast_id,
	m.uuid,
	m.text,
	m.attachments,
	m.quick_replies,
	m.locale,
	m.created_on,
	m.direction,
	m.status,
	m.visibility,
	m.msg_count,
	m.error_count,
	m.next_attempt,
	m.failed_reason,
	m.high_priority,
	m.external_id,
	m.metadata,
	m.channel_id,
	m.contact_id,
	m.contact_urn_id,
	m.org_id,
	u.identity AS "urn_urn",
	u.auth AS "urn_auth"
FROM
	msgs_msg m
INNER JOIN 
	contacts_contacturn u ON u.id = m.contact_urn_id
INNER JOIN 
	channels_channel c ON c.id = m.channel_id
WHERE
	m.direction = 'O' AND m.status IN ('I', 'E') AND m.next_attempt <= NOW() AND c.is_active = TRUE
ORDER BY
    m.next_attempt ASC, m.created_on ASC
LIMIT 5000`

// GetMessagesForRetry gets errored outgoing messages scheduled for retry, with an active channel
func GetMessagesForRetry(ctx context.Context, db Queryer) ([]*Msg, error) {
	return loadMessages(ctx, db, loadMessagesForRetrySQL)
}

func loadMessages(ctx context.Context, db Queryer, sql string, params ...interface{}) ([]*Msg, error) {
	rows, err := db.QueryxContext(ctx, sql, params...)
	if err != nil {
		return nil, errors.Wrapf(err, "error querying msgs")
	}
	defer rows.Close()

	msgs := make([]*Msg, 0)

	for rows.Next() {
		msg := &Msg{}
		err = rows.StructScan(&msg.m)
		if err != nil {
			return nil, errors.Wrap(err, "error scanning msg row")
		}

		msgs = append(msgs, msg)
	}

	return msgs, nil
}

// NormalizeAttachment will turn any relative URL in the passed in attachment and normalize it to
// include the full host for attachment domains
func NormalizeAttachment(cfg *runtime.Config, attachment utils.Attachment) utils.Attachment {
	// don't try to modify geo type attachments which are just coordinates
	if attachment.ContentType() == "geo" {
		return attachment
	}

	url := attachment.URL()
	if !strings.HasPrefix(url, "http") {
		if strings.HasPrefix(url, "/") {
			url = fmt.Sprintf("https://%s%s", cfg.AttachmentDomain, url)
		} else {
			url = fmt.Sprintf("https://%s/%s", cfg.AttachmentDomain, url)
		}
	}
	return utils.Attachment(fmt.Sprintf("%s:%s", attachment.ContentType(), url))
}

// SetTimeout sets the timeout for this message
func (m *Msg) SetTimeout(start time.Time, timeout time.Duration) {
	m.SessionWaitStartedOn = &start
	m.SessionTimeout = int(timeout / time.Second)
}

// InsertMessages inserts the passed in messages in a single query
func InsertMessages(ctx context.Context, tx Queryer, msgs []*Msg) error {
	is := make([]interface{}, len(msgs))
	for i := range msgs {
		is[i] = &msgs[i].m
	}

	return BulkQuery(ctx, "insert messages", tx, sqlInsertMsgSQL, is)
}

const sqlInsertMsgSQL = `
INSERT INTO
msgs_msg(uuid, text, attachments, quick_replies, locale, high_priority, created_on, modified_on, queued_on, sent_on, direction, status, metadata,
		 visibility, msg_type, msg_count, error_count, next_attempt, failed_reason, channel_id,
		 contact_id, contact_urn_id, org_id, flow_id, broadcast_id, created_by_id)
  VALUES(:uuid, :text, :attachments, :quick_replies, :locale, :high_priority, :created_on, now(), now(), :sent_on, :direction, :status, :metadata,
		 :visibility, :msg_type, :msg_count, :error_count, :next_attempt, :failed_reason, :channel_id,
		 :contact_id, :contact_urn_id, :org_id, :flow_id, :broadcast_id, :created_by_id)
RETURNING 
	id AS id, 
	modified_on AS modified_on,
	queued_on AS queued_on
`

// MarkMessageHandled updates a message after handling
func MarkMessageHandled(ctx context.Context, tx Queryer, msgID MsgID, status MsgStatus, visibility MsgVisibility, flow FlowID, attachments []utils.Attachment, logUUIDs []ChannelLogUUID) error {
	_, err := tx.ExecContext(ctx,
		`UPDATE msgs_msg SET status = $2, visibility = $3, flow_id = $4, attachments = $5, log_uuids = array_cat(log_uuids, $6) WHERE id = $1`,
		msgID, status, visibility, flow, pq.Array(attachments), pq.Array(logUUIDs),
	)
	return errors.Wrapf(err, "error marking msg #%d as handled", msgID)
}

// MarkMessagesForRequeuing marks the passed in messages as initializing(I) with a next attempt value
// so that the retry messages task will pick them up.
func MarkMessagesForRequeuing(ctx context.Context, db Queryer, msgs []*Msg) error {
	nextAttempt := time.Now().Add(10 * time.Minute)
	return updateMessageStatus(ctx, db, msgs, MsgStatusInitializing, &nextAttempt)
}

// MarkMessagesQueued marks the passed in messages as queued(Q)
func MarkMessagesQueued(ctx context.Context, db Queryer, msgs []*Msg) error {
	return updateMessageStatus(ctx, db, msgs, MsgStatusQueued, nil)
}

const sqlUpdateMsgStatus = `
UPDATE msgs_msg
   SET status = m.status, next_attempt = m.next_attempt::timestamp with time zone
  FROM (VALUES(:id, :status, :next_attempt)) AS m(id, status, next_attempt)
 WHERE msgs_msg.id = m.id::bigint`

func updateMessageStatus(ctx context.Context, db Queryer, msgs []*Msg, status MsgStatus, nextAttempt *time.Time) error {
	is := make([]interface{}, len(msgs))
	for i, msg := range msgs {
		m := &msg.m
		m.Status = status
		m.NextAttempt = nextAttempt
		is[i] = m
	}

	return BulkQuery(ctx, "updating message status", db, sqlUpdateMsgStatus, is)
}

const sqlUpdateMsgForResending = `
UPDATE msgs_msg m
   SET channel_id = r.channel_id::int,
       status = 'Q',
       error_count = 0,
       failed_reason = NULL,
       queued_on = r.queued_on::timestamp with time zone,
       sent_on = NULL,
       modified_on = NOW()
  FROM (VALUES(:id, :channel_id, :queued_on)) AS r(id, channel_id, queued_on)
 WHERE m.id = r.id::bigint`

const sqlUpdateMsgResendFailed = `
UPDATE msgs_msg m
   SET channel_id = NULL, status = 'F', error_count = 0, failed_reason = 'D', sent_on = NULL, modified_on = NOW()
 WHERE id = ANY($1)`

// ResendMessages prepares messages for resending by reselecting a channel and marking them as PENDING
func ResendMessages(ctx context.Context, db Queryer, rp *redis.Pool, oa *OrgAssets, msgs []*Msg) ([]*Msg, error) {
	channels := oa.SessionAssets().Channels()

	// for the bulk db updates
	resends := make([]interface{}, 0, len(msgs))
	refails := make([]MsgID, 0, len(msgs))

	resent := make([]*Msg, 0, len(msgs))

	for _, msg := range msgs {
		var ch *flows.Channel
		urnID := msg.ContactURNID()

		if urnID != nil {
			// reselect channel for this message's URN
			urn, err := URNForID(ctx, db, oa, *urnID)
			if err != nil {
				return nil, errors.Wrap(err, "error loading URN")
			}
			msg.m.URN = urn // needs to be set for queueing to courier

			contactURN, err := flows.ParseRawURN(channels, urn, assets.IgnoreMissing)
			if err != nil {
				return nil, errors.Wrap(err, "error parsing URN")
			}

			ch = channels.GetForURN(contactURN, assets.ChannelRoleSend)
		}

		if ch != nil {
			channel := oa.ChannelByUUID(ch.UUID())
			msg.m.ChannelID = channel.ID()
			msg.m.Status = MsgStatusPending
			msg.m.QueuedOn = dates.Now()
			msg.m.SentOn = nil
			msg.m.ErrorCount = 0
			msg.m.FailedReason = ""
			msg.IsResend = true // mark message as being a resend so it will be queued to courier as such

			resends = append(resends, msg.m)
			resent = append(resent, msg)
		} else {
			// if we don't have channel or a URN, fail again
			msg.m.ChannelID = NilChannelID
			msg.m.Status = MsgStatusFailed
			msg.m.QueuedOn = dates.Now()
			msg.m.SentOn = nil
			msg.m.ErrorCount = 0
			msg.m.FailedReason = MsgFailedNoDestination

			refails = append(refails, MsgID(msg.m.ID))
		}
	}

	// update the messages that can be resent
	err := BulkQuery(ctx, "updating messages for resending", db, sqlUpdateMsgForResending, resends)
	if err != nil {
		return nil, errors.Wrapf(err, "error updating messages for resending")
	}

	// and update the messages that can't be
	_, err = db.ExecContext(ctx, sqlUpdateMsgResendFailed, pq.Array(refails))
	if err != nil {
		return nil, errors.Wrapf(err, "error updating non-resendable messages")
	}

	return resent, nil
}

const sqlFailChannelMessages = `
WITH rows AS (
	SELECT id FROM msgs_msg
	WHERE org_id = $1 AND direction = 'O' AND channel_id = $2 AND status IN ('P', 'Q', 'E') 
	LIMIT 1000
)
UPDATE msgs_msg SET status = 'F', failed_reason = $3, modified_on = NOW() WHERE id IN (SELECT id FROM rows)`

func FailChannelMessages(ctx context.Context, db Queryer, orgID OrgID, channelID ChannelID, failedReason MsgFailedReason) error {
	for {
		// and update the messages as FAILED
		res, err := db.ExecContext(ctx, sqlFailChannelMessages, orgID, channelID, failedReason)
		if err != nil {
			return err
		}
		rows, _ := res.RowsAffected()
		if rows == 0 {
			break
		}
	}
	return nil
}

func NewMsgOut(oa *OrgAssets, c *flows.Contact, text string, atts []utils.Attachment, qrs []string, locale envs.Locale) (*flows.MsgOut, *Channel) {
	// resolve URN + channel for this contact
	urn := urns.NilURN
	var channel *Channel
	var channelRef *assets.ChannelReference
	for _, dest := range c.ResolveDestinations(false) {
		urn = dest.URN.URN()
		channel = oa.ChannelByUUID(dest.Channel.UUID())
		channelRef = dest.Channel.Reference()
		break
	}

	// is this message sendable?
	unsendableReason := flows.NilUnsendableReason
	if c.Status() != flows.ContactStatusActive {
		unsendableReason = flows.UnsendableReasonContactStatus
	} else if urn == urns.NilURN || channel == nil {
		unsendableReason = flows.UnsendableReasonNoDestination
	}

	return flows.NewMsgOut(urn, channelRef, text, atts, qrs, nil, flows.NilMsgTopic, locale, unsendableReason), channel
}

// NilID implementations

func (i *MsgID) Scan(value any) error         { return null.ScanInt(value, i) }
func (i MsgID) Value() (driver.Value, error)  { return null.IntValue(i) }
func (i *MsgID) UnmarshalJSON(b []byte) error { return null.UnmarshalInt(b, i) }
func (i MsgID) MarshalJSON() ([]byte, error)  { return null.MarshalInt(i) }

func (i *BroadcastID) Scan(value any) error         { return null.ScanInt(value, i) }
func (i BroadcastID) Value() (driver.Value, error)  { return null.IntValue(i) }
func (i *BroadcastID) UnmarshalJSON(b []byte) error { return null.UnmarshalInt(b, i) }
func (i BroadcastID) MarshalJSON() ([]byte, error)  { return null.MarshalInt(i) }

func (s MsgFailedReason) Value() (driver.Value, error) { return null.StringValue(s) }
func (s *MsgFailedReason) Scan(value any) error        { return null.ScanString(value, s) }
