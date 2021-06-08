package models

import (
	"encoding/json"
	"time"

	"github.com/nyaruka/gocommon/dates"
)

type TicketEventID int
type TicketEventType string

const (
	TicketEventTypeOpened TicketEventType = "O"
	TicketEventTypeClosed TicketEventType = "C"
)

type TicketEvent struct {
	e struct {
		ID          TicketEventID   `json:"id"                      db:"id"`
		OrgID       OrgID           `json:"org_id"                  db:"org_id"`
		TicketID    TicketID        `json:"ticket_id"               db:"ticket_id"`
		EventType   TicketEventType `json:"event_type"              db:"event_type"`
		CreatedByID UserID          `json:"created_by_id,omitempty" db:"created_by_id"`
		CreatedOn   time.Time       `json:"created_on"              db:"created_on"`
	}
}

func NewTicketEvent(orgID OrgID, ticketID TicketID, eventType TicketEventType) *TicketEvent {
	event := &TicketEvent{}
	e := &event.e

	e.OrgID = orgID
	e.TicketID = ticketID
	e.EventType = eventType
	e.CreatedOn = dates.Now()
	return event
}

func (e *TicketEvent) ID() TicketEventID          { return e.e.ID }
func (e *TicketEvent) OrgID() OrgID               { return e.e.OrgID }
func (e *TicketEvent) TicketID() TicketID         { return e.e.TicketID }
func (e *TicketEvent) EventType() TicketEventType { return e.e.EventType }

// MarshalJSON is our custom marshaller so that our inner struct get output
func (e *TicketEvent) MarshalJSON() ([]byte, error) {
	return json.Marshal(e.e)
}

// UnmarshalJSON is our custom marshaller so that our inner struct get output
func (e *TicketEvent) UnmarshalJSON(b []byte) error {
	return json.Unmarshal(b, &e.e)
}
