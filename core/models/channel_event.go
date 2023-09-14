package models

import (
	"context"
	"encoding/json"
	"time"

	"github.com/nyaruka/null/v3"
)

type ChannelEventType string
type ChannelEventID int64

// channel event types
const (
	EventTypeNewConversation ChannelEventType = "new_conversation"
	EventTypeWelcomeMessage  ChannelEventType = "welcome_message"
	EventTypeReferral        ChannelEventType = "referral"
	EventTypeMissedCall      ChannelEventType = "mo_miss"
	EventTypeIncomingCall    ChannelEventType = "mo_call"
	EventTypeStopContact     ChannelEventType = "stop_contact"
	EventTypeOptIn           ChannelEventType = "optin"
	EventTypeOptOut          ChannelEventType = "optout"
)

// ContactSeenEvents are those which count as the contact having been seen
var ContactSeenEvents = map[ChannelEventType]bool{
	EventTypeNewConversation: true,
	EventTypeReferral:        true,
	EventTypeMissedCall:      true,
	EventTypeIncomingCall:    true,
	EventTypeStopContact:     true,
	EventTypeOptIn:           true,
	EventTypeOptOut:          true,
}

// ChannelEvent represents an event that occurred associated with a channel, such as a referral, missed call, etc..
type ChannelEvent struct {
	e struct {
		ID         ChannelEventID   `json:"id"           db:"id"`
		EventType  ChannelEventType `json:"event_type"   db:"event_type"`
		OrgID      OrgID            `json:"org_id"       db:"org_id"`
		ChannelID  ChannelID        `json:"channel_id"   db:"channel_id"`
		ContactID  ContactID        `json:"contact_id"   db:"contact_id"`
		URNID      URNID            `json:"urn_id"       db:"contact_urn_id"`
		Extra      null.Map[any]    `json:"extra"        db:"extra"`
		OccurredOn time.Time        `json:"occurred_on"  db:"occurred_on"`

		// only in JSON representation
		NewContact bool `json:"new_contact"`

		// only in DB representation
		CreatedOn time.Time `db:"created_on"`
	}
}

func (e *ChannelEvent) ID() ChannelEventID    { return e.e.ID }
func (e *ChannelEvent) ContactID() ContactID  { return e.e.ContactID }
func (e *ChannelEvent) URNID() URNID          { return e.e.URNID }
func (e *ChannelEvent) OrgID() OrgID          { return e.e.OrgID }
func (e *ChannelEvent) ChannelID() ChannelID  { return e.e.ChannelID }
func (e *ChannelEvent) IsNewContact() bool    { return e.e.NewContact }
func (e *ChannelEvent) OccurredOn() time.Time { return e.e.OccurredOn }
func (e *ChannelEvent) Extra() map[string]any { return e.e.Extra }
func (e *ChannelEvent) ExtraString(key string) string {
	asStr, ok := e.e.Extra[key].(string)
	if ok {
		return asStr
	}
	return ""
}

// MarshalJSON is our custom marshaller so that our inner struct get output
func (e *ChannelEvent) MarshalJSON() ([]byte, error) {
	return json.Marshal(e.e)
}

// UnmarshalJSON is our custom marshaller so that our inner struct get output
func (e *ChannelEvent) UnmarshalJSON(b []byte) error {
	return json.Unmarshal(b, &e.e)
}

const sqlInsertChannelEvent = `
INSERT INTO channels_channelevent(event_type, extra, occurred_on, created_on, channel_id, contact_id, contact_urn_id, org_id)
	 VALUES(:event_type, :extra, :occurred_on, :created_on, :channel_id, :contact_id, :contact_urn_id, :org_id)
  RETURNING id`

// Insert inserts this channel event to our DB. The ID of the channel event will be
// set if no error is returned
func (e *ChannelEvent) Insert(ctx context.Context, db DBorTx) error {
	return BulkQuery(ctx, "insert channel event", db, sqlInsertChannelEvent, []any{&e.e})
}

// NewChannelEvent creates a new channel event for the passed in parameters, returning it
func NewChannelEvent(eventType ChannelEventType, orgID OrgID, channelID ChannelID, contactID ContactID, urnID URNID, extra map[string]any, isNewContact bool) *ChannelEvent {
	event := &ChannelEvent{}
	e := &event.e

	e.EventType = eventType
	e.OrgID = orgID
	e.ChannelID = channelID
	e.ContactID = contactID
	e.URNID = urnID
	e.NewContact = isNewContact

	if extra == nil {
		e.Extra = null.Map[any]{}
	} else {
		e.Extra = null.Map[any](extra)
	}

	now := time.Now()
	e.CreatedOn = now
	e.OccurredOn = now

	return event
}
