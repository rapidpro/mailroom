package models

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/jmoiron/sqlx/types"
	"github.com/lib/pq"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/config"
	"github.com/nyaruka/mailroom/gsm7"
	"github.com/pkg/errors"
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

type BroadcastID int64

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

// TODO: response_to_id, response_to_external_id

// Msg is our type for mailroom messages
type Msg struct {
	m struct {
		ID                   flows.MsgID        `db:"id"              json:"id"`
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
		Metadata             types.JSONText     `db:"metadata"        json:"metadata"`
		ChannelID            ChannelID          `db:"channel_id"      json:"channel_id"`
		ChannelUUID          assets.ChannelUUID `                     json:"channel_uuid"`
		ConnectionID         ConnectionID       `db:"connection_id"`
		ContactID            flows.ContactID    `db:"contact_id"      json:"contact_id"`
		ContactURNID         URNID              `db:"contact_urn_id"  json:"contact_urn_id"`
		ResponseToID         null.Int           `db:"response_to_id"  json:"response_to_id"`
		ResponseToExternalID string             `                     json:"response_to_external_id"`
		URN                  urns.URN           `                     json:"urn"`
		URNAuth              string             `                     json:"urn_auth,omitempty"`
		OrgID                OrgID              `db:"org_id"          json:"org_id"`
		TopupID              TopupID            `db:"topup_id"`

		// These three fields are set on the last outgoing message in a session's sprint. In the case
		// of the session being at a wait with a timeout then the timeout will be set. It is up to
		// Courier to update the session's timeout appropriately after sending the message.
		SessionID            SessionID  `json:"session_id,omitempty"`
		SessionWaitStartedOn *time.Time `json:"session_wait_started_on,omitempty"`
		SessionTimeout       int        `json:"session_timeout,omitempty"`
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
func (m *Msg) Metadata() types.JSONText        { return m.m.Metadata }
func (m *Msg) MsgCount() int                   { return m.m.MsgCount }
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

// SetResponseTo set the incoming message that this session should be associated with in this sprint
func (m *Msg) SetResponseTo(id null.Int, externalID string) {
	m.m.ResponseToID = id
	m.m.ResponseToExternalID = externalID
}

func (m *Msg) MarshalJSON() ([]byte, error) {
	return json.Marshal(m.m)
}

// NewOutgoingMsg creates an outgoing message for the passed in flow message. Note that this message is created in a queued state!
func NewOutgoingMsg(orgID OrgID, channel *Channel, contactID flows.ContactID, out *flows.MsgOut, createdOn time.Time) (*Msg, error) {
	_, _, query, _ := out.URN().ToParts()
	parsedQuery, err := url.ParseQuery(query)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to parse urn: %s", out.URN())
	}

	// get the id of our URN
	idQuery := parsedQuery.Get("id")
	urnID, err := strconv.Atoi(idQuery)
	if urnID == 0 {
		return nil, errors.Wrapf(err, "unable to create msg for URN, has no id: %s", out.URN())
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
			// if our URL is relative, remap it to something fully qualified
			url := a.URL()
			if !strings.HasPrefix(url, "http") {
				if strings.HasPrefix(url, "/") {
					url = fmt.Sprintf("https://%s%s", config.Mailroom.AttachmentDomain, url)
				} else {
					url = fmt.Sprintf("https://%s/%s", config.Mailroom.AttachmentDomain, url)
				}
			}
			m.Attachments = append(m.Attachments, fmt.Sprintf("%s:%s", a.ContentType(), url))
		}
	}

	// if we have quick replies, populate our metadata
	if len(out.QuickReplies()) > 0 {
		metadata := make(map[string]interface{})
		metadata["quick_replies"] = out.QuickReplies()

		metadataJSON, err := json.Marshal(metadata)
		if err != nil {
			return nil, errors.Wrap(err, "error marshalling quick replies")
		}
		m.Metadata = metadataJSON
	}

	// set URN auth info if we have any (this is used when queuing later on)
	urnAuth := parsedQuery.Get("auth")
	if urnAuth != "" {
		m.URNAuth = urnAuth
	}

	// calculate msg count
	if m.URN.Scheme() == urns.TelScheme {
		m.MsgCount = gsm7.Segments(m.Text) + len(m.Attachments)
	} else {
		m.MsgCount = 1
	}

	return msg, nil
}

// SetTimeout sets the timeout for this message
func (m *Msg) SetTimeout(id SessionID, start time.Time, timeout time.Duration) {
	m.m.SessionID = id
	m.m.SessionWaitStartedOn = &start
	m.m.SessionTimeout = int(timeout / time.Second)
}

// InsertMessages inserts the passed in messages in a single query
func InsertMessages(ctx context.Context, tx Queryer, msgs []*Msg) error {
	is := make([]interface{}, len(msgs))
	for i := range msgs {
		is[i] = &msgs[i].m
	}

	return BulkSQL(ctx, "insert messages", tx, insertMsgSQL, is)
}

const insertMsgSQL = `
INSERT INTO
msgs_msg(uuid, text, high_priority, created_on, modified_on, queued_on, direction, status, attachments, metadata,
		 visibility, msg_type, msg_count, error_count, next_attempt, channel_id, response_to_id,
		 contact_id, contact_urn_id, org_id, topup_id)
  VALUES(:uuid, :text, :high_priority, :created_on, now(), now(), :direction, :status, :attachments, :metadata,
		 :visibility, :msg_type, :msg_count, :error_count, :next_attempt, :channel_id, :response_to_id,
		 :contact_id, :contact_urn_id, :org_id, :topup_id)
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
	ids := make([]int, len(msgs))
	for i, m := range msgs {
		ids[i] = int(m.m.ID)
	}

	q, vs, err := sqlx.In(updateMsgStatusSQL, ids, status)
	if err != nil {
		return errors.Wrap(err, "error preparing query for updating message status")
	}
	q = tx.Rebind(q)

	_, err = tx.ExecContext(ctx, q, vs...)
	if err != nil {
		return errors.Wrap(err, "error updating message status")
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

// BroadcastTranslation is the translation for the passed in language
type BroadcastTranslation struct {
	Text         string             `json:"text"`
	Attachments  []flows.Attachment `json:"attachments,omitempty"`
	QuickReplies []string           `json:"quick_replies,omitempty"`
}

// Broadcast represents a broadcast that needs to be sent
type Broadcast struct {
	b struct {
		Translations map[utils.Language]*BroadcastTranslation `json:"translations"`
		BaseLanguage utils.Language                           `json:"base_language"`
		URNs         []urns.URN                               `json:"urns,omitempty"`
		ContactIDs   []flows.ContactID                        `json:"contact_ids,omitempty"`
		GroupIDs     []GroupID                                `json:"group_ids,omitempty"`
		OrgID        OrgID                                    `json:"org_id"`
	}
}

func (b *Broadcast) ContactIDs() []flows.ContactID                          { return b.b.ContactIDs }
func (b *Broadcast) GroupIDs() []GroupID                                    { return b.b.GroupIDs }
func (b *Broadcast) URNs() []urns.URN                                       { return b.b.URNs }
func (b *Broadcast) OrgID() OrgID                                           { return b.b.OrgID }
func (b *Broadcast) Translations() map[utils.Language]*BroadcastTranslation { return b.b.Translations }

func (b *Broadcast) MarshalJSON() ([]byte, error)    { return json.Marshal(b.b) }
func (b *Broadcast) UnmarshalJSON(data []byte) error { return json.Unmarshal(data, &b.b) }

// NewBroadcastFromEvent creates a broadcast object from the passed in broadcast event
func NewBroadcastFromEvent(ctx context.Context, tx Queryer, org *OrgAssets, event *events.BroadcastCreatedEvent) (*Broadcast, error) {
	bcast := &Broadcast{}
	bcast.b.OrgID = org.OrgID()
	bcast.b.BaseLanguage = event.BaseLanguage
	bcast.b.URNs = event.URNs
	bcast.b.Translations = make(map[utils.Language]*BroadcastTranslation)
	for l, t := range event.Translations {
		bcast.b.Translations[l] = &BroadcastTranslation{
			Text:         t.Text,
			Attachments:  t.Attachments,
			QuickReplies: t.QuickReplies,
		}
	}

	// resolve our contact references
	contactIDs, err := ContactIDsFromReferences(ctx, tx, org, event.Contacts)
	if err != nil {
		return nil, errors.Wrapf(err, "error resolving contact references")
	}
	bcast.b.ContactIDs = contactIDs

	// and our groups
	groups := make([]GroupID, 0, len(event.Groups))
	for i := range event.Groups {
		group := org.GroupByUUID(event.Groups[i].UUID)
		if group != nil {
			groups = append(groups, group.ID())
		}
	}
	bcast.b.GroupIDs = groups

	return bcast, nil
}

func (b *Broadcast) CreateBatch(contactIDs []flows.ContactID) *BroadcastBatch {
	batch := &BroadcastBatch{}
	batch.b.BaseLanguage = b.b.BaseLanguage
	batch.b.Translations = b.b.Translations
	batch.b.OrgID = b.b.OrgID
	batch.b.ContactIDs = contactIDs
	return batch
}

// BroadcastBatch represents a batch of contacts that need messages sent for
type BroadcastBatch struct {
	b struct {
		Translations map[utils.Language]*BroadcastTranslation `json:"translations"`
		BaseLanguage utils.Language                           `json:"base_language"`
		URNs         map[flows.ContactID]urns.URN             `json:"urns,omitempty"`
		ContactIDs   []flows.ContactID                        `json:"contact_ids,omitempty"`
		IsLast       bool                                     `json:"is_last"`
		OrgID        OrgID                                    `json:"org_id"`
	}
}

func (b *BroadcastBatch) ContactIDs() []flows.ContactID             { return b.b.ContactIDs }
func (b *BroadcastBatch) URNs() map[flows.ContactID]urns.URN        { return b.b.URNs }
func (b *BroadcastBatch) SetURNs(urns map[flows.ContactID]urns.URN) { b.b.URNs = urns }
func (b *BroadcastBatch) OrgID() OrgID                              { return b.b.OrgID }
func (b *BroadcastBatch) Translations() map[utils.Language]*BroadcastTranslation {
	return b.b.Translations
}
func (b *BroadcastBatch) BaseLanguage() utils.Language { return b.b.BaseLanguage }
func (b *BroadcastBatch) IsLast() bool                 { return b.b.IsLast }
func (b *BroadcastBatch) SetIsLast(last bool)          { b.b.IsLast = last }

func (b *BroadcastBatch) MarshalJSON() ([]byte, error)    { return json.Marshal(b.b) }
func (b *BroadcastBatch) UnmarshalJSON(data []byte) error { return json.Unmarshal(data, &b.b) }

func CreateBroadcastMessages(ctx context.Context, db *sqlx.DB, org *OrgAssets, sa flows.SessionAssets, bcast *BroadcastBatch) ([]*Msg, error) {
	repeatedContacts := make(map[flows.ContactID]bool)
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
	contacts, err := LoadContacts(ctx, db, org, contactIDs)
	if err != nil {
		return nil, errors.Wrapf(err, "error loading contacts for broadcast")
	}

	channels := sa.Channels()

	// for each contact, build our message
	msgs := make([]*Msg, 0, len(contacts))

	// utility method to build up our message
	buildMessage := func(c *Contact, forceURN urns.URN) (*Msg, error) {
		if c.IsStopped() || c.IsBlocked() {
			return nil, nil
		}

		contact, err := c.FlowContact(org, sa)
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
					channel = org.ChannelByUUID(c.UUID())
					break
				}
			}
		} else {
			// no forced URN, find the first URN we can send to
			for _, u := range contact.URNs() {
				c := channels.GetForURN(u, assets.ChannelRoleSend)
				if c != nil {
					urn = u.URN()
					channel = org.ChannelByUUID(c.UUID())
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
		if lang != utils.NilLanguage {
			found := false
			for _, l := range org.Env().AllowedLanguages() {
				if l == lang {
					found = true
					break
				}
			}
			if !found {
				lang = utils.NilLanguage
			}
		}

		// have a valid contact language, try that
		trans := bcast.Translations()
		t := trans[lang]

		// not found? try org default language
		if t == nil {
			t = trans[org.Env().DefaultLanguage()]
		}

		// not found? use broadcast base language
		if t == nil {
			t = trans[bcast.BaseLanguage()]
		}

		if t == nil {
			logrus.WithField("base_language", bcast.BaseLanguage()).WithField("translations", trans).Error("unable to find translation for broadcast")
			return nil, nil
		}

		// create our outgoing message
		out := flows.NewMsgOut(urn, channel.ChannelReference(), t.Text, t.Attachments, t.QuickReplies)
		msg, err := NewOutgoingMsg(org.OrgID(), channel, contact.ID(), out, time.Now())
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

	// insert them in a single request
	err = InsertMessages(ctx, db, msgs)
	if err != nil {
		return nil, errors.Wrapf(err, "error inserting broadcast messages")
	}

	return msgs, nil
}
