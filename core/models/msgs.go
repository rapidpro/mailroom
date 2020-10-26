package models

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/nyaruka/gocommon/gsm7"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/goflow/excellent"
	"github.com/nyaruka/goflow/excellent/types"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/definition/legacy/expressions"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/config"
	"github.com/nyaruka/null"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/lib/pq/hstore"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// MsgID is our internal type for msg ids, which can be null/0
type MsgID null.Int

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
	TypeInbox = MsgType("I")
	TypeFlow  = MsgType("F")
	TypeIVR   = MsgType("V")
	TypeUSSD  = MsgType("U")
)

type MsgStatus string

// BroadcastID is our internal type for broadcast ids, which can be null/0
type BroadcastID null.Int

// NilBroadcastID is our constant for a nil broadcast id
const NilBroadcastID = BroadcastID(0)

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

// TemplateState represents what state are templates are in, either already evaluated, not evaluated or
// that they are unevaluated legacy templates
type TemplateState string

const (
	TemplateStateEvaluated   = TemplateState("evaluated")
	TemplateStateLegacy      = TemplateState("legacy")
	TemplateStateUnevaluated = TemplateState("unevaluated")
)

// Msg is our type for mailroom messages
type Msg struct {
	m struct {
		ID                   flows.MsgID        `db:"id"              json:"id"`
		BroadcastID          BroadcastID        `db:"broadcast_id"    json:"broadcast_id,omitempty"`
		UUID                 flows.MsgUUID      `db:"uuid"            json:"uuid"`
		Text                 string             `db:"text"            json:"text"`
		HighPriority         bool               `db:"high_priority"   json:"high_priority"`
		CreatedOn            time.Time          `db:"created_on"      json:"created_on"`
		ModifiedOn           time.Time          `db:"modified_on"     json:"modified_on"`
		SentOn               time.Time          `db:"sent_on"         json:"sent_on"`
		QueuedOn             time.Time          `db:"queued_on"       json:"queued_on"`
		Direction            MsgDirection       `db:"direction"       json:"direction"`
		Status               MsgStatus          `db:"status"          json:"status"`
		Visibility           MsgVisibility      `db:"visibility"      json:"visibility"`
		MsgType              MsgType            `db:"msg_type"`
		MsgCount             int                `db:"msg_count"       json:"tps_cost"`
		ErrorCount           int                `db:"error_count"     json:"error_count"`
		NextAttempt          time.Time          `db:"next_attempt"    json:"next_attempt"`
		ExternalID           null.String        `db:"external_id"     json:"external_id"`
		Attachments          pq.StringArray     `db:"attachments"     json:"attachments"`
		Metadata             null.Map           `db:"metadata"        json:"metadata,omitempty"`
		ChannelID            ChannelID          `db:"channel_id"      json:"channel_id"`
		ChannelUUID          assets.ChannelUUID `                     json:"channel_uuid"`
		ConnectionID         *ConnectionID      `db:"connection_id"`
		ContactID            ContactID          `db:"contact_id"      json:"contact_id"`
		ContactURNID         *URNID             `db:"contact_urn_id"  json:"contact_urn_id"`
		ResponseToID         MsgID              `db:"response_to_id"  json:"response_to_id"`
		ResponseToExternalID null.String        `                     json:"response_to_external_id"`
		URN                  urns.URN           `                     json:"urn"`
		URNAuth              null.String        `                     json:"urn_auth,omitempty"`
		OrgID                OrgID              `db:"org_id"          json:"org_id"`
		TopupID              TopupID            `db:"topup_id"`

		SessionID     SessionID     `json:"session_id,omitempty"`
		SessionStatus SessionStatus `json:"session_status,omitempty"`

		// These fields are set on the last outgoing message in a session's sprint. In the case
		// of the session being at a wait with a timeout then the timeout will be set. It is up to
		// Courier to update the session's timeout appropriately after sending the message.
		SessionWaitStartedOn *time.Time `json:"session_wait_started_on,omitempty"`
		SessionTimeout       int        `json:"session_timeout,omitempty"`
	}

	channel *Channel
}

func (m *Msg) ID() flows.MsgID                  { return m.m.ID }
func (m *Msg) BroadcastID() BroadcastID         { return m.m.BroadcastID }
func (m *Msg) UUID() flows.MsgUUID              { return m.m.UUID }
func (m *Msg) Channel() *Channel                { return m.channel }
func (m *Msg) Text() string                     { return m.m.Text }
func (m *Msg) HighPriority() bool               { return m.m.HighPriority }
func (m *Msg) CreatedOn() time.Time             { return m.m.CreatedOn }
func (m *Msg) ModifiedOn() time.Time            { return m.m.ModifiedOn }
func (m *Msg) SentOn() time.Time                { return m.m.SentOn }
func (m *Msg) QueuedOn() time.Time              { return m.m.QueuedOn }
func (m *Msg) Direction() MsgDirection          { return m.m.Direction }
func (m *Msg) Status() MsgStatus                { return m.m.Status }
func (m *Msg) Visibility() MsgVisibility        { return m.m.Visibility }
func (m *Msg) MsgType() MsgType                 { return m.m.MsgType }
func (m *Msg) ErrorCount() int                  { return m.m.ErrorCount }
func (m *Msg) NextAttempt() time.Time           { return m.m.NextAttempt }
func (m *Msg) ExternalID() null.String          { return m.m.ExternalID }
func (m *Msg) Metadata() map[string]interface{} { return m.m.Metadata.Map() }
func (m *Msg) MsgCount() int                    { return m.m.MsgCount }
func (m *Msg) ChannelID() ChannelID             { return m.m.ChannelID }
func (m *Msg) ChannelUUID() assets.ChannelUUID  { return m.m.ChannelUUID }
func (m *Msg) ConnectionID() *ConnectionID      { return m.m.ConnectionID }
func (m *Msg) URN() urns.URN                    { return m.m.URN }
func (m *Msg) URNAuth() null.String             { return m.m.URNAuth }
func (m *Msg) OrgID() OrgID                     { return m.m.OrgID }
func (m *Msg) TopupID() TopupID                 { return m.m.TopupID }
func (m *Msg) ContactID() ContactID             { return m.m.ContactID }
func (m *Msg) ContactURNID() *URNID             { return m.m.ContactURNID }

func (m *Msg) SetTopup(topupID TopupID)               { m.m.TopupID = topupID }
func (m *Msg) SetChannelID(channelID ChannelID)       { m.m.ChannelID = channelID }
func (m *Msg) SetBroadcastID(broadcastID BroadcastID) { m.m.BroadcastID = broadcastID }

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

// SetResponseTo set the incoming message that this session should be associated with in this sprint
func (m *Msg) SetResponseTo(id MsgID, externalID null.String) {
	m.m.ResponseToID = id
	m.m.ResponseToExternalID = externalID

	if id != NilMsgID || externalID != "" {
		m.m.HighPriority = true
	}
}

func (m *Msg) MarshalJSON() ([]byte, error) {
	return json.Marshal(m.m)
}

// NewIncomingIVR creates a new incoming IVR message for the passed in text and attachment
func NewIncomingIVR(orgID OrgID, conn *ChannelConnection, in *flows.MsgIn, createdOn time.Time) *Msg {
	msg := &Msg{}
	m := &msg.m

	msg.SetURN(in.URN())
	m.UUID = in.UUID()
	m.Text = in.Text()
	m.Direction = DirectionIn
	m.Status = MsgStatusHandled
	m.Visibility = VisibilityVisible
	m.MsgType = TypeIVR
	m.ContactID = conn.ContactID()

	urnID := conn.ContactURNID()
	m.ContactURNID = &urnID

	connID := conn.ID()
	m.ConnectionID = &connID

	m.OrgID = orgID
	m.TopupID = NilTopupID
	m.CreatedOn = createdOn

	msg.SetChannelID(conn.ChannelID())

	// add any attachments
	for _, a := range in.Attachments() {
		m.Attachments = append(m.Attachments, string(NormalizeAttachment(a)))
	}

	return msg
}

// NewOutgoingIVR creates a new IVR message for the passed in text with the optional attachment
func NewOutgoingIVR(orgID OrgID, conn *ChannelConnection, out *flows.MsgOut, createdOn time.Time) (*Msg, error) {
	msg := &Msg{}
	m := &msg.m

	msg.SetURN(out.URN())
	m.UUID = out.UUID()
	m.Text = out.Text()
	m.HighPriority = false
	m.Direction = DirectionOut
	m.Status = MsgStatusWired
	m.Visibility = VisibilityVisible
	m.MsgType = TypeIVR
	m.ContactID = conn.ContactID()

	urnID := conn.ContactURNID()
	m.ContactURNID = &urnID

	connID := conn.ID()
	m.ConnectionID = &connID

	m.URN = out.URN()

	m.OrgID = orgID
	m.TopupID = NilTopupID
	m.CreatedOn = createdOn
	msg.SetChannelID(conn.ChannelID())

	// if we have attachments, add them
	for _, a := range out.Attachments() {
		m.Attachments = append(m.Attachments, string(NormalizeAttachment(a)))
	}

	return msg, nil
}

// NewOutgoingMsg creates an outgoing message for the passed in flow message.
func NewOutgoingMsg(org *Org, channel *Channel, contactID ContactID, out *flows.MsgOut, createdOn time.Time) (*Msg, error) {
	msg := &Msg{}
	m := &msg.m

	// we fail messages for suspended orgs right away
	status := MsgStatusQueued
	if org.Suspended() {
		status = MsgStatusFailed
	}

	m.UUID = out.UUID()
	m.Text = out.Text()
	m.HighPriority = false
	m.Direction = DirectionOut
	m.Status = status
	m.Visibility = VisibilityVisible
	m.MsgType = TypeFlow
	m.ContactID = contactID
	m.OrgID = org.ID()
	m.TopupID = NilTopupID
	m.CreatedOn = createdOn

	err := msg.SetURN(out.URN())
	if err != nil {
		return nil, errors.Wrapf(err, "error setting msg urn")
	}

	if channel != nil {
		m.ChannelUUID = channel.UUID()
		msg.SetChannelID(channel.ID())
		msg.channel = channel
	}

	m.MsgCount = 1

	// if we have attachments, add them
	if len(out.Attachments()) > 0 {
		for _, a := range out.Attachments() {
			m.Attachments = append(m.Attachments, string(NormalizeAttachment(a)))
		}
	}

	// populate metadata if we have any
	if len(out.QuickReplies()) > 0 || out.Templating() != nil || out.Topic() != flows.NilMsgTopic {
		metadata := make(map[string]interface{})
		if len(out.QuickReplies()) > 0 {
			metadata["quick_replies"] = out.QuickReplies()
		}
		if out.Templating() != nil {
			metadata["templating"] = out.Templating()
		}
		if out.Topic() != flows.NilMsgTopic {
			metadata["topic"] = string(out.Topic())
		}
		m.Metadata = null.NewMap(metadata)
	}

	// calculate msg count
	if m.URN.Scheme() == urns.TelScheme {
		m.MsgCount = gsm7.Segments(m.Text) + len(m.Attachments)
	} else {
		m.MsgCount = 1
	}

	return msg, nil
}

// NewIncomingMsg creates a new incoming message for the passed in text and attachment
func NewIncomingMsg(orgID OrgID, channel *Channel, contactID ContactID, in *flows.MsgIn, createdOn time.Time) *Msg {
	msg := &Msg{}
	m := &msg.m

	msg.SetURN(in.URN())
	m.UUID = in.UUID()
	m.Text = in.Text()
	m.Direction = DirectionIn
	m.Status = MsgStatusHandled
	m.Visibility = VisibilityVisible
	m.MsgType = TypeFlow
	m.ContactID = contactID

	m.OrgID = orgID
	m.TopupID = NilTopupID
	m.CreatedOn = createdOn

	if channel != nil {
		msg.SetChannelID(channel.ID())
		m.ChannelUUID = channel.UUID()
		msg.channel = channel
	}

	// add any attachments
	for _, a := range in.Attachments() {
		m.Attachments = append(m.Attachments, string(NormalizeAttachment(a)))
	}

	return msg
}

// NormalizeAttachment will turn any relative URL in the passed in attachment and normalize it to
// include the full host for attachment domains
func NormalizeAttachment(attachment utils.Attachment) utils.Attachment {
	// don't try to modify geo type attachments which are just coordinates
	if attachment.ContentType() == "geo" {
		return attachment
	}

	url := attachment.URL()
	if !strings.HasPrefix(url, "http") {
		if strings.HasPrefix(url, "/") {
			url = fmt.Sprintf("https://%s%s", config.Mailroom.AttachmentDomain, url)
		} else {
			url = fmt.Sprintf("https://%s/%s", config.Mailroom.AttachmentDomain, url)
		}
	}
	return utils.Attachment(fmt.Sprintf("%s:%s", attachment.ContentType(), url))
}

func (m *Msg) SetSession(id SessionID, status SessionStatus) {
	m.m.SessionID = id
	m.m.SessionStatus = status
}

// SetTimeout sets the timeout for this message
func (m *Msg) SetTimeout(start time.Time, timeout time.Duration) {
	m.m.SessionWaitStartedOn = &start
	m.m.SessionTimeout = int(timeout / time.Second)
}

// InsertMessages inserts the passed in messages in a single query
func InsertMessages(ctx context.Context, tx Queryer, msgs []*Msg) error {
	is := make([]interface{}, len(msgs))
	for i := range msgs {
		is[i] = &msgs[i].m
	}

	return BulkQuery(ctx, "insert messages", tx, insertMsgSQL, is)
}

const insertMsgSQL = `
INSERT INTO
msgs_msg(uuid, text, high_priority, created_on, modified_on, queued_on, direction, status, attachments, metadata,
		 visibility, msg_type, msg_count, error_count, next_attempt, channel_id, connection_id, response_to_id,
		 contact_id, contact_urn_id, org_id, topup_id, broadcast_id)
  VALUES(:uuid, :text, :high_priority, :created_on, now(), now(), :direction, :status, :attachments, :metadata,
		 :visibility, :msg_type, :msg_count, :error_count, :next_attempt, :channel_id, :connection_id, :response_to_id,
		 :contact_id, :contact_urn_id, :org_id, :topup_id, :broadcast_id)
RETURNING 
	id as id, 
	now() as modified_on,
	now() as queued_on
`

// UpdateMessage updates the passed in message status, visibility and msg type
func UpdateMessage(ctx context.Context, tx Queryer, msgID flows.MsgID, status MsgStatus, visibility MsgVisibility, msgType MsgType, topup TopupID) error {
	_, err := tx.ExecContext(
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
		return errors.Wrapf(err, "error updating msg: %d", msgID)
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
	is := make([]interface{}, len(msgs))
	for i, msg := range msgs {
		m := &msg.m
		m.Status = status
		is[i] = m
	}

	return BulkQuery(ctx, "updating message status", tx, updateMsgStatusSQL, is)
}

const updateMsgStatusSQL = `
UPDATE 
	msgs_msg
SET
	status = m.status
FROM (
	VALUES(:id, :status)
) AS
	m(id, status)
WHERE
	msgs_msg.id = m.id::int
`

// GetMessageIDFromUUID gets the ID of a message from its UUID
func GetMessageIDFromUUID(ctx context.Context, db Queryer, uuid flows.MsgUUID) (MsgID, error) {
	var id MsgID
	err := db.GetContext(ctx, &id, `SELECT id FROM msgs_msg WHERE uuid = $1`, uuid)
	if err != nil {
		return NilMsgID, errors.Wrapf(err, "error querying id for msg with uuid '%s'", uuid)
	}
	return id, nil
}

// BroadcastTranslation is the translation for the passed in language
type BroadcastTranslation struct {
	Text         string             `json:"text"`
	Attachments  []utils.Attachment `json:"attachments,omitempty"`
	QuickReplies []string           `json:"quick_replies,omitempty"`
}

// Broadcast represents a broadcast that needs to be sent
type Broadcast struct {
	b struct {
		BroadcastID   BroadcastID                             `json:"broadcast_id,omitempty" db:"id"`
		Translations  map[envs.Language]*BroadcastTranslation `json:"translations"`
		Text          hstore.Hstore                           `                              db:"text"`
		TemplateState TemplateState                           `json:"template_state"`
		BaseLanguage  envs.Language                           `json:"base_language"          db:"base_language"`
		URNs          []urns.URN                              `json:"urns,omitempty"`
		ContactIDs    []ContactID                             `json:"contact_ids,omitempty"`
		GroupIDs      []GroupID                               `json:"group_ids,omitempty"`
		OrgID         OrgID                                   `json:"org_id"                 db:"org_id"`
		ParentID      BroadcastID                             `json:"parent_id,omitempty"    db:"parent_id"`
	}
}

func (b *Broadcast) BroadcastID() BroadcastID                              { return b.b.BroadcastID }
func (b *Broadcast) ContactIDs() []ContactID                               { return b.b.ContactIDs }
func (b *Broadcast) GroupIDs() []GroupID                                   { return b.b.GroupIDs }
func (b *Broadcast) URNs() []urns.URN                                      { return b.b.URNs }
func (b *Broadcast) OrgID() OrgID                                          { return b.b.OrgID }
func (b *Broadcast) BaseLanguage() envs.Language                           { return b.b.BaseLanguage }
func (b *Broadcast) Translations() map[envs.Language]*BroadcastTranslation { return b.b.Translations }
func (b *Broadcast) TemplateState() TemplateState                          { return b.b.TemplateState }

func (b *Broadcast) MarshalJSON() ([]byte, error)    { return json.Marshal(b.b) }
func (b *Broadcast) UnmarshalJSON(data []byte) error { return json.Unmarshal(data, &b.b) }

// NewBroadcast creates a new broadcast with the passed in parameters
func NewBroadcast(
	orgID OrgID, id BroadcastID, translations map[envs.Language]*BroadcastTranslation,
	state TemplateState, baseLanguage envs.Language, urns []urns.URN, contactIDs []ContactID, groupIDs []GroupID) *Broadcast {

	bcast := &Broadcast{}
	bcast.b.OrgID = orgID
	bcast.b.BroadcastID = id
	bcast.b.Translations = translations
	bcast.b.TemplateState = state
	bcast.b.BaseLanguage = baseLanguage
	bcast.b.URNs = urns
	bcast.b.ContactIDs = contactIDs
	bcast.b.GroupIDs = groupIDs

	return bcast
}

// InsertChildBroadcast clones the passed in broadcast as a parent, then inserts that broadcast into the DB
func InsertChildBroadcast(ctx context.Context, db Queryer, parent *Broadcast) (*Broadcast, error) {
	child := NewBroadcast(
		parent.OrgID(),
		NilBroadcastID,
		parent.b.Translations,
		parent.b.TemplateState,
		parent.b.BaseLanguage,
		parent.b.URNs,
		parent.b.ContactIDs,
		parent.b.GroupIDs,
	)
	// populate our parent id
	child.b.ParentID = parent.BroadcastID()

	// populate text from our translations
	child.b.Text.Map = make(map[string]sql.NullString)
	for lang, t := range child.b.Translations {
		child.b.Text.Map[string(lang)] = sql.NullString{String: t.Text, Valid: true}
		if len(t.Attachments) > 0 || len(t.QuickReplies) > 0 {
			return nil, errors.Errorf("cannot clone broadcast with quick replies or attachments")
		}
	}

	// insert our broadcast
	err := BulkQuery(ctx, "inserting broadcast", db, insertBroadcastSQL, []interface{}{&child.b})
	if err != nil {
		return nil, errors.Wrapf(err, "error inserting child broadcast for broadcast: %d", parent.BroadcastID())
	}

	// build up all our contact associations
	contacts := make([]interface{}, 0, len(child.b.ContactIDs))
	for _, contactID := range child.b.ContactIDs {
		contacts = append(contacts, &broadcastContact{
			BroadcastID: child.BroadcastID(),
			ContactID:   contactID,
		})
	}

	// insert our contacts
	err = BulkQuery(ctx, "inserting broadcast contacts", db, insertBroadcastContactsSQL, contacts)
	if err != nil {
		return nil, errors.Wrapf(err, "error inserting contacts for broadcast")
	}

	// build up all our group associations
	groups := make([]interface{}, 0, len(child.b.GroupIDs))
	for _, groupID := range child.b.GroupIDs {
		groups = append(groups, &broadcastGroup{
			BroadcastID: child.BroadcastID(),
			GroupID:     groupID,
		})
	}

	// insert our groups
	err = BulkQuery(ctx, "inserting broadcast groups", db, insertBroadcastGroupsSQL, groups)
	if err != nil {
		return nil, errors.Wrapf(err, "error inserting groups for broadcast")
	}

	// finally our URNs
	urns := make([]interface{}, 0, len(child.b.URNs))
	for _, urn := range child.b.URNs {
		urnID := GetURNID(urn)
		if urnID == NilURNID {
			return nil, errors.Errorf("attempt to insert new broadcast with URNs that do not have id: %s", urn)
		}
		urns = append(urns, &broadcastURN{
			BroadcastID: child.BroadcastID(),
			URNID:       urnID,
		})
	}

	// insert our urns
	err = BulkQuery(ctx, "inserting broadcast urns", db, insertBroadcastURNsSQL, urns)
	if err != nil {
		return nil, errors.Wrapf(err, "error inserting URNs for broadcast")
	}

	return child, nil
}

type broadcastURN struct {
	BroadcastID BroadcastID `db:"broadcast_id"`
	URNID       URNID       `db:"contacturn_id"`
}

type broadcastContact struct {
	BroadcastID BroadcastID `db:"broadcast_id"`
	ContactID   ContactID   `db:"contact_id"`
}

type broadcastGroup struct {
	BroadcastID BroadcastID `db:"broadcast_id"`
	GroupID     GroupID     `db:"contactgroup_id"`
}

const insertBroadcastSQL = `
INSERT INTO
	msgs_broadcast( org_id,  parent_id, is_active, created_on, modified_on, status,  text,  base_language, send_all)
			VALUES(:org_id, :parent_id, TRUE,      NOW()     , NOW(),       'Q',    :text, :base_language, FALSE)
RETURNING
	id
`

const insertBroadcastContactsSQL = `
INSERT INTO
	msgs_broadcast_contacts( broadcast_id,  contact_id)
	                 VALUES(:broadcast_id,     :contact_id)
`

const insertBroadcastGroupsSQL = `
INSERT INTO
	msgs_broadcast_groups( broadcast_id,  contactgroup_id)
	               VALUES(:broadcast_id,     :contactgroup_id)
`

const insertBroadcastURNsSQL = `
INSERT INTO
	msgs_broadcast_urns( broadcast_id,  contacturn_id)
	             VALUES(:broadcast_id, :contacturn_id)
`

// NewBroadcastFromEvent creates a broadcast object from the passed in broadcast event
func NewBroadcastFromEvent(ctx context.Context, tx Queryer, org *OrgAssets, event *events.BroadcastCreatedEvent) (*Broadcast, error) {
	// converst our translations to our type
	translations := make(map[envs.Language]*BroadcastTranslation)
	for l, t := range event.Translations {
		translations[l] = &BroadcastTranslation{
			Text:         t.Text,
			Attachments:  t.Attachments,
			QuickReplies: t.QuickReplies,
		}
	}

	// resolve our contact references
	contactIDs, err := GetContactIDsFromReferences(ctx, tx, org.OrgID(), event.Contacts)
	if err != nil {
		return nil, errors.Wrapf(err, "error resolving contact references")
	}

	// and our groups
	groupIDs := make([]GroupID, 0, len(event.Groups))
	for i := range event.Groups {
		group := org.GroupByUUID(event.Groups[i].UUID)
		if group != nil {
			groupIDs = append(groupIDs, group.ID())
		}
	}

	return NewBroadcast(org.OrgID(), NilBroadcastID, translations, TemplateStateEvaluated, event.BaseLanguage, event.URNs, contactIDs, groupIDs), nil
}

func (b *Broadcast) CreateBatch(contactIDs []ContactID) *BroadcastBatch {
	batch := &BroadcastBatch{}
	batch.b.BroadcastID = b.b.BroadcastID
	batch.b.BaseLanguage = b.b.BaseLanguage
	batch.b.Translations = b.b.Translations
	batch.b.TemplateState = b.b.TemplateState
	batch.b.OrgID = b.b.OrgID
	batch.b.ContactIDs = contactIDs
	return batch
}

// BroadcastBatch represents a batch of contacts that need messages sent for
type BroadcastBatch struct {
	b struct {
		BroadcastID   BroadcastID                             `json:"broadcast_id,omitempty"`
		Translations  map[envs.Language]*BroadcastTranslation `json:"translations"`
		BaseLanguage  envs.Language                           `json:"base_language"`
		TemplateState TemplateState                           `json:"template_state"`
		URNs          map[ContactID]urns.URN                  `json:"urns,omitempty"`
		ContactIDs    []ContactID                             `json:"contact_ids,omitempty"`
		IsLast        bool                                    `json:"is_last"`
		OrgID         OrgID                                   `json:"org_id"`
	}
}

func (b *BroadcastBatch) BroadcastID() BroadcastID            { return b.b.BroadcastID }
func (b *BroadcastBatch) ContactIDs() []ContactID             { return b.b.ContactIDs }
func (b *BroadcastBatch) URNs() map[ContactID]urns.URN        { return b.b.URNs }
func (b *BroadcastBatch) SetURNs(urns map[ContactID]urns.URN) { b.b.URNs = urns }
func (b *BroadcastBatch) OrgID() OrgID                        { return b.b.OrgID }
func (b *BroadcastBatch) Translations() map[envs.Language]*BroadcastTranslation {
	return b.b.Translations
}
func (b *BroadcastBatch) TemplateState() TemplateState { return b.b.TemplateState }
func (b *BroadcastBatch) BaseLanguage() envs.Language  { return b.b.BaseLanguage }
func (b *BroadcastBatch) IsLast() bool                 { return b.b.IsLast }
func (b *BroadcastBatch) SetIsLast(last bool)          { b.b.IsLast = last }

func (b *BroadcastBatch) MarshalJSON() ([]byte, error)    { return json.Marshal(b.b) }
func (b *BroadcastBatch) UnmarshalJSON(data []byte) error { return json.Unmarshal(data, &b.b) }

func CreateBroadcastMessages(ctx context.Context, db Queryer, rp *redis.Pool, oa *OrgAssets, bcast *BroadcastBatch) ([]*Msg, error) {
	repeatedContacts := make(map[ContactID]bool)
	broadcastURNs := bcast.URNs()

	// build our list of contact ids
	contactIDs := bcast.ContactIDs()

	// build a map of the contacts that are present both in our URN list and our contact id list
	if broadcastURNs != nil {
		for _, id := range contactIDs {
			_, found := broadcastURNs[id]
			if found {
				repeatedContacts[id] = true
			}
		}
	}

	// if we have URN we need to send to, add those contacts as well if not already repeated
	if broadcastURNs != nil {
		for id := range broadcastURNs {
			if !repeatedContacts[id] {
				contactIDs = append(contactIDs, id)
			}
		}
	}

	// load all our contacts
	contacts, err := LoadContacts(ctx, db, oa, contactIDs)
	if err != nil {
		return nil, errors.Wrapf(err, "error loading contacts for broadcast")
	}

	channels := oa.SessionAssets().Channels()

	// for each contact, build our message
	msgs := make([]*Msg, 0, len(contacts))

	// utility method to build up our message
	buildMessage := func(c *Contact, forceURN urns.URN) (*Msg, error) {
		if c.Status() != ContactStatusActive {
			return nil, nil
		}

		contact, err := c.FlowContact(oa)
		if err != nil {
			return nil, errors.Wrapf(err, "error creating flow contact")
		}

		urn := urns.NilURN
		var channel *Channel

		// we are forcing to send to a non-preferred URN, find the channel
		if forceURN != urns.NilURN {
			for _, u := range contact.URNs() {
				if u.URN().Identity() == forceURN.Identity() {
					c := channels.GetForURN(u, assets.ChannelRoleSend)
					if c == nil {
						return nil, nil
					}
					urn = u.URN()
					channel = oa.ChannelByUUID(c.UUID())
					break
				}
			}
		} else {
			// no forced URN, find the first URN we can send to
			for _, u := range contact.URNs() {
				c := channels.GetForURN(u, assets.ChannelRoleSend)
				if c != nil {
					urn = u.URN()
					channel = oa.ChannelByUUID(c.UUID())
					break
				}
			}
		}

		// no urn and channel? move on
		if channel == nil {
			return nil, nil
		}

		// resolve our translations, the order is:
		//   1) valid contact language
		//   2) org default language
		//   3) broadcast base language
		lang := contact.Language()
		if lang != envs.NilLanguage {
			found := false
			for _, l := range oa.Env().AllowedLanguages() {
				if l == lang {
					found = true
					break
				}
			}
			if !found {
				lang = envs.NilLanguage
			}
		}

		// have a valid contact language, try that
		trans := bcast.Translations()
		t := trans[lang]

		// not found? try org default language
		if t == nil {
			t = trans[oa.Env().DefaultLanguage()]
		}

		// not found? use broadcast base language
		if t == nil {
			t = trans[bcast.BaseLanguage()]
		}

		if t == nil {
			logrus.WithField("base_language", bcast.BaseLanguage()).WithField("translations", trans).Error("unable to find translation for broadcast")
			return nil, nil
		}

		template := ""

		// if this is a legacy template, migrate it forward
		if bcast.TemplateState() == TemplateStateLegacy {
			template, _ = expressions.MigrateTemplate(t.Text, nil)
		} else if bcast.TemplateState() == TemplateStateUnevaluated {
			template = t.Text
		}

		text := t.Text

		// if we have a template, evaluate it
		if template != "" {
			// build up the minimum viable context for templates
			templateCtx := types.NewXObject(map[string]types.XValue{
				"contact": flows.Context(oa.Env(), contact),
				"fields":  flows.Context(oa.Env(), contact.Fields()),
				"globals": flows.Context(oa.Env(), oa.SessionAssets().Globals()),
				"urns":    flows.ContextFunc(oa.Env(), contact.URNs().MapContext),
			})
			text, _ = excellent.EvaluateTemplate(oa.Env(), templateCtx, template, nil)
		}

		// don't do anything if we have no text or attachments
		if text == "" && len(t.Attachments) == 0 {
			return nil, nil
		}

		// create our outgoing message
		out := flows.NewMsgOut(urn, channel.ChannelReference(), text, t.Attachments, t.QuickReplies, nil, flows.NilMsgTopic)
		msg, err := NewOutgoingMsg(oa.Org(), channel, c.ID(), out, time.Now())
		msg.SetBroadcastID(bcast.BroadcastID())
		if err != nil {
			return nil, errors.Wrapf(err, "error creating outgoing message")
		}

		return msg, nil
	}

	// run through all our contacts to create our messages
	for _, c := range contacts {
		// use the preferred URN if present
		urn := broadcastURNs[c.ID()]
		msg, err := buildMessage(c, urn)
		if err != nil {
			return nil, errors.Wrapf(err, "error creating broadcast message")
		}
		if msg != nil {
			msgs = append(msgs, msg)
		}

		// if this is a contact that will receive two messages, calculate that one as well
		if repeatedContacts[c.ID()] {
			m2, err := buildMessage(c, urns.NilURN)
			if err != nil {
				return nil, errors.Wrapf(err, "error creating broadcast message")
			}

			// add this message if it isn't a duplicate
			if m2 != nil && m2.URN() != msg.URN() {
				msgs = append(msgs, m2)
			}
		}
	}

	// allocate a topup for these message if org uses topups
	topup, err := AllocateTopups(ctx, db, rp, oa.Org(), len(msgs))
	if err != nil {
		return nil, errors.Wrapf(err, "error allocating topup for broadcast messages")
	}

	// if we have an active topup, assign it to our messages
	if topup != NilTopupID {
		for _, m := range msgs {
			m.SetTopup(topup)
		}
	}

	// insert them in a single request
	err = InsertMessages(ctx, db, msgs)
	if err != nil {
		return nil, errors.Wrapf(err, "error inserting broadcast messages")
	}

	return msgs, nil
}

// MarkBroadcastSent marks the passed in broadcast as sent
func MarkBroadcastSent(ctx context.Context, db Queryer, id BroadcastID) error {
	// noop if it is a nil id
	if id == NilBroadcastID {
		return nil
	}

	_, err := db.ExecContext(ctx, `UPDATE msgs_broadcast SET status = 'S', modified_on = now() WHERE id = $1`, id)
	if err != nil {
		return errors.Wrapf(err, "error setting broadcast with id %d as sent", id)
	}
	return nil
}

// NilID implementations

// MarshalJSON marshals into JSON. 0 values will become null
func (i MsgID) MarshalJSON() ([]byte, error) {
	return null.Int(i).MarshalJSON()
}

// UnmarshalJSON unmarshals from JSON. null values become 0
func (i *MsgID) UnmarshalJSON(b []byte) error {
	return null.UnmarshalInt(b, (*null.Int)(i))
}

// Value returns the db value, null is returned for 0
func (i MsgID) Value() (driver.Value, error) {
	return null.Int(i).Value()
}

// Scan scans from the db value. null values become 0
func (i *MsgID) Scan(value interface{}) error {
	return null.ScanInt(value, (*null.Int)(i))
}

// MarshalJSON marshals into JSON. 0 values will become null
func (i BroadcastID) MarshalJSON() ([]byte, error) {
	return null.Int(i).MarshalJSON()
}

// UnmarshalJSON unmarshals from JSON. null values become 0
func (i *BroadcastID) UnmarshalJSON(b []byte) error {
	return null.UnmarshalInt(b, (*null.Int)(i))
}

// Value returns the db value, null is returned for 0
func (i BroadcastID) Value() (driver.Value, error) {
	return null.Int(i).Value()
}

// Scan scans from the db value. null values become 0
func (i *BroadcastID) Scan(value interface{}) error {
	return null.ScanInt(value, (*null.Int)(i))
}
