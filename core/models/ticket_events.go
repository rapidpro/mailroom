package models

import (
	"context"
	"encoding/json"
	"time"

	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/null"
)

type TicketEventID int
type TicketEventType string

const (
	TicketEventTypeOpened       TicketEventType = "O"
	TicketEventTypeAssigned     TicketEventType = "A"
	TicketEventTypeNoteAdded    TicketEventType = "N"
	TicketEventTypeTopicChanged TicketEventType = "T"
	TicketEventTypeClosed       TicketEventType = "C"
	TicketEventTypeReopened     TicketEventType = "R"
)

type TicketEvent struct {
	e struct {
		ID          TicketEventID   `json:"id"                      db:"id"`
		OrgID       OrgID           `json:"org_id"                  db:"org_id"`
		ContactID   ContactID       `json:"contact_id"              db:"contact_id"`
		TicketID    TicketID        `json:"ticket_id"               db:"ticket_id"`
		EventType   TicketEventType `json:"event_type"              db:"event_type"`
		Note        null.String     `json:"note,omitempty"          db:"note"`
		TopicID     TopicID         `json:"topic_id,omitempty"   db:"topic_id"`
		AssigneeID  UserID          `json:"assignee_id,omitempty"   db:"assignee_id"`
		CreatedByID UserID          `json:"created_by_id,omitempty" db:"created_by_id"`
		CreatedOn   time.Time       `json:"created_on"              db:"created_on"`
	}
}

func NewTicketOpenedEvent(t *Ticket, userID UserID, assigneeID UserID) *TicketEvent {
	return newTicketEvent(t, userID, TicketEventTypeOpened, "", NilTopicID, assigneeID)
}

func NewTicketAssignedEvent(t *Ticket, userID UserID, assigneeID UserID, note string) *TicketEvent {
	return newTicketEvent(t, userID, TicketEventTypeAssigned, note, NilTopicID, assigneeID)
}

func NewTicketNoteAddedEvent(t *Ticket, userID UserID, note string) *TicketEvent {
	return newTicketEvent(t, userID, TicketEventTypeNoteAdded, note, NilTopicID, NilUserID)
}

func NewTicketTopicChangedEvent(t *Ticket, userID UserID, topicID TopicID) *TicketEvent {
	return newTicketEvent(t, userID, TicketEventTypeTopicChanged, "", topicID, NilUserID)
}

func NewTicketClosedEvent(t *Ticket, userID UserID) *TicketEvent {
	return newTicketEvent(t, userID, TicketEventTypeClosed, "", NilTopicID, NilUserID)
}

func NewTicketReopenedEvent(t *Ticket, userID UserID) *TicketEvent {
	return newTicketEvent(t, userID, TicketEventTypeReopened, "", NilTopicID, NilUserID)
}

func newTicketEvent(t *Ticket, userID UserID, eventType TicketEventType, note string, topicID TopicID, assigneeID UserID) *TicketEvent {
	event := &TicketEvent{}
	e := &event.e
	e.OrgID = t.OrgID()
	e.ContactID = t.ContactID()
	e.TicketID = t.ID()
	e.EventType = eventType
	e.Note = null.String(note)
	e.TopicID = topicID
	e.AssigneeID = assigneeID
	e.CreatedOn = dates.Now()
	e.CreatedByID = userID
	return event
}

func (e *TicketEvent) ID() TicketEventID          { return e.e.ID }
func (e *TicketEvent) OrgID() OrgID               { return e.e.OrgID }
func (e *TicketEvent) ContactID() ContactID       { return e.e.ContactID }
func (e *TicketEvent) TicketID() TicketID         { return e.e.TicketID }
func (e *TicketEvent) EventType() TicketEventType { return e.e.EventType }
func (e *TicketEvent) Note() null.String          { return e.e.Note }
func (e *TicketEvent) TopicID() TopicID           { return e.e.TopicID }
func (e *TicketEvent) AssigneeID() UserID         { return e.e.AssigneeID }
func (e *TicketEvent) CreatedByID() UserID        { return e.e.CreatedByID }

// MarshalJSON is our custom marshaller so that our inner struct get output
func (e *TicketEvent) MarshalJSON() ([]byte, error) {
	return json.Marshal(e.e)
}

// UnmarshalJSON is our custom marshaller so that our inner struct get output
func (e *TicketEvent) UnmarshalJSON(b []byte) error {
	return json.Unmarshal(b, &e.e)
}

const sqlInsertTicketEvents = `
INSERT INTO
	tickets_ticketevent(org_id,  contact_id,  ticket_id,  event_type,  note,  topic_id,  assignee_id,  created_on,  created_by_id)
	            VALUES(:org_id, :contact_id, :ticket_id, :event_type, :note, :topic_id, :assignee_id, :created_on, :created_by_id)
RETURNING
	id
`

func InsertTicketEvents(ctx context.Context, db Queryer, evts []*TicketEvent) error {
	// convert to interface arrray
	is := make([]interface{}, len(evts))
	for i := range evts {
		is[i] = &evts[i].e
	}

	return BulkQuery(ctx, "inserting ticket events", db, sqlInsertTicketEvents, is)
}
