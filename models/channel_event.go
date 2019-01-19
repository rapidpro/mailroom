package models

import (
	"encoding/json"

	"github.com/nyaruka/goflow/flows"
)

type ChannelEventType string
type ChannelEventID int64

const (
	NewConversationEventType = ChannelEventType("new_conversation")
	ReferralEventType        = ChannelEventType("referral")
	MOMissEventType          = ChannelEventType("mo_miss")
	MOCallEventType          = ChannelEventType("mo_call")
)

// ChannelEvent represents an event that occurred associated with a channel, such as a referral, missed call, etc..
type ChannelEvent struct {
	e struct {
		ID         ChannelEventID    `json:"id"           db:"id"`
		EventType  ChannelEventType  `json:"event_type"   db:"event_type"`
		OrgID      OrgID             `json:"org_id"       db:"org_id"`
		ChannelID  ChannelID         `json:"channel_id"   db:"channel_id"`
		ContactID  flows.ContactID   `json:"contact_id"   db:"contact_id"`
		URNID      URNID             `json:"urn_id"       db:"contact_urn_id"`
		Extra      map[string]string `json:"extra"        db:"extra"`
		NewContact bool              `json:"new_contact"`
	}
}

func (e *ChannelEvent) ID() ChannelEventID         { return e.e.ID }
func (e *ChannelEvent) ContactID() flows.ContactID { return e.e.ContactID }
func (e *ChannelEvent) URNID() URNID               { return e.e.URNID }
func (e *ChannelEvent) OrgID() OrgID               { return e.e.OrgID }
func (e *ChannelEvent) ChannelID() ChannelID       { return e.e.ChannelID }
func (e *ChannelEvent) Extra() map[string]string   { return e.e.Extra }
func (e *ChannelEvent) IsNewContact() bool         { return e.e.NewContact }

// MarshalJSON is our custom marshaller so that our inner struct get output
func (e *ChannelEvent) MarshalJSON() ([]byte, error) {
	return json.Marshal(e.e)
}

// UnmarshalJSON is our custom marshaller so that our inner struct get output
func (e *ChannelEvent) UnmarshalJSON(b []byte) error {
	return json.Unmarshal(b, &e.e)
}

func NewChannelEvent(eventType ChannelEventType, orgID OrgID, channelID ChannelID, contactID flows.ContactID, urnID URNID, extra map[string]string, isNew bool) *ChannelEvent {
	event := &ChannelEvent{}
	e := &event.e

	e.EventType = eventType
	e.OrgID = orgID
	e.ChannelID = channelID
	e.ContactID = contactID
	e.URNID = urnID
	e.Extra = extra
	e.NewContact = isNew

	return event
}
