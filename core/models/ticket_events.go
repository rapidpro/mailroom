package models

import (
	"context"
	"encoding/json"
	"time"

	"github.com/nyaruka/gocommon/dates"
)

type TicketEventID int
type TicketEventType string

const (
	TicketEventTypeOpened   TicketEventType = "O"
	TicketEventTypeAssigned TicketEventType = "A"
	TicketEventTypeNote     TicketEventType = "N"
	TicketEventTypeClosed   TicketEventType = "C"
	TicketEventTypeReopened TicketEventType = "R"
)

type TicketEvent struct {
	e struct {
		ID          TicketEventID   `json:"id"                      db:"id"`
		OrgID       OrgID           `json:"org_id"                  db:"org_id"`
		ContactID   ContactID       `json:"contact_id"              db:"contact_id"`
		TicketID    TicketID        `json:"ticket_id"               db:"ticket_id"`
		EventType   TicketEventType `json:"event_type"              db:"event_type"`
		CreatedByID UserID          `json:"created_by_id,omitempty" db:"created_by_id"`
		CreatedOn   time.Time       `json:"created_on"              db:"created_on"`
	}
}

func NewTicketEvent(orgID OrgID, userID UserID, contactID ContactID, ticketID TicketID, eventType TicketEventType) *TicketEvent {
	event := &TicketEvent{}
	e := &event.e

	e.OrgID = orgID
	e.ContactID = contactID
	e.TicketID = ticketID
	e.EventType = eventType
	e.CreatedOn = dates.Now()
	e.CreatedByID = userID
	return event
}

func (e *TicketEvent) ID() TicketEventID          { return e.e.ID }
func (e *TicketEvent) OrgID() OrgID               { return e.e.OrgID }
func (e *TicketEvent) ContactID() ContactID       { return e.e.ContactID }
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

const insertTicketEventsSQL = `
INSERT INTO
	tickets_ticketevent(org_id, contact_id, ticket_id, event_type, created_on, created_by_id)
	VALUES(:org_id, :contact_id, :ticket_id, :event_type, :created_on, :created_by_id)
RETURNING
	id
`

func InsertTicketEvents(ctx context.Context, db Queryer, evts []*TicketEvent) error {
	// convert to interface arrray
	is := make([]interface{}, len(evts))
	for i := range evts {
		is[i] = &evts[i].e
	}

	return BulkQuery(ctx, "inserting ticket events", db, insertTicketEventsSQL, is)
}
