package models

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/null/v3"
	"github.com/pkg/errors"
)

type TicketID int

// NilTicketID is our constant for a nil ticket id
const NilTicketID = TicketID(0)

func (i *TicketID) Scan(value any) error         { return null.ScanInt(value, i) }
func (i TicketID) Value() (driver.Value, error)  { return null.IntValue(i) }
func (i *TicketID) UnmarshalJSON(b []byte) error { return null.UnmarshalInt(b, i) }
func (i TicketID) MarshalJSON() ([]byte, error)  { return null.MarshalInt(i) }

type TicketerID int
type TicketStatus string
type TicketDailyCountType string
type TicketDailyTimingType string

const (
	TicketStatusOpen   = TicketStatus("O")
	TicketStatusClosed = TicketStatus("C")

	TicketDailyCountOpening    = TicketDailyCountType("O")
	TicketDailyCountAssignment = TicketDailyCountType("A")
	TicketDailyCountReply      = TicketDailyCountType("R")

	TicketDailyTimingFirstReply = TicketDailyTimingType("R")
	TicketDailyTimingLastClose  = TicketDailyTimingType("C")
)

type Ticket struct {
	t struct {
		ID             TicketID         `db:"id"`
		UUID           flows.TicketUUID `db:"uuid"`
		OrgID          OrgID            `db:"org_id"`
		ContactID      ContactID        `db:"contact_id"`
		Status         TicketStatus     `db:"status"`
		TopicID        TopicID          `db:"topic_id"`
		Body           string           `db:"body"`
		AssigneeID     UserID           `db:"assignee_id"`
		OpenedOn       time.Time        `db:"opened_on"`
		OpenedByID     UserID           `db:"opened_by_id"`
		OpenedInID     FlowID           `db:"opened_in_id"`
		RepliedOn      *time.Time       `db:"replied_on"`
		ModifiedOn     time.Time        `db:"modified_on"`
		ClosedOn       *time.Time       `db:"closed_on"`
		LastActivityOn time.Time        `db:"last_activity_on"`
	}
}

// NewTicket creates a new open ticket
func NewTicket(uuid flows.TicketUUID, orgID OrgID, userID UserID, flowID FlowID, contactID ContactID, topicID TopicID, body string, assigneeID UserID) *Ticket {
	t := &Ticket{}
	t.t.UUID = uuid
	t.t.OrgID = orgID
	t.t.OpenedByID = userID
	t.t.OpenedInID = flowID
	t.t.ContactID = contactID
	t.t.Status = TicketStatusOpen
	t.t.TopicID = topicID
	t.t.Body = body
	t.t.AssigneeID = assigneeID
	return t
}

func (t *Ticket) ID() TicketID              { return t.t.ID }
func (t *Ticket) UUID() flows.TicketUUID    { return t.t.UUID }
func (t *Ticket) OrgID() OrgID              { return t.t.OrgID }
func (t *Ticket) ContactID() ContactID      { return t.t.ContactID }
func (t *Ticket) Status() TicketStatus      { return t.t.Status }
func (t *Ticket) TopicID() TopicID          { return t.t.TopicID }
func (t *Ticket) Body() string              { return t.t.Body }
func (t *Ticket) AssigneeID() UserID        { return t.t.AssigneeID }
func (t *Ticket) RepliedOn() *time.Time     { return t.t.RepliedOn }
func (t *Ticket) LastActivityOn() time.Time { return t.t.LastActivityOn }
func (t *Ticket) OpenedByID() UserID        { return t.t.OpenedByID }

func (t *Ticket) FlowTicket(oa *OrgAssets) *flows.Ticket {
	var topic *flows.Topic
	if t.TopicID() != NilTopicID {
		dbTopic := oa.TopicByID(t.TopicID())
		if dbTopic != nil {
			topic = oa.SessionAssets().Topics().Get(dbTopic.UUID())
		}
	}

	var assignee *flows.User
	if t.AssigneeID() != NilUserID {
		user := oa.UserByID(t.AssigneeID())
		if user != nil {
			assignee = oa.SessionAssets().Users().Get(user.Email())
		}
	}

	return flows.NewTicket(t.UUID(), topic, t.Body(), assignee)
}

const sqlSelectLastOpenTicket = `
SELECT
  id,
  uuid,
  org_id,
  contact_id,
  status,
  topic_id,
  body,
  assignee_id,
  opened_on,
  opened_by_id,
  opened_in_id,
  replied_on,
  modified_on,
  closed_on,
  last_activity_on
    FROM tickets_ticket
   WHERE contact_id = $1 AND status = 'O'
ORDER BY opened_on DESC
   LIMIT 1`

// LoadOpenTicketForContact looks up the last opened open ticket for the passed in contact
func LoadOpenTicketForContact(ctx context.Context, db *sqlx.DB, contact *Contact) (*Ticket, error) {
	tickets, err := loadTickets(ctx, db, sqlSelectLastOpenTicket, contact.ID())
	if err != nil {
		return nil, err
	}
	if len(tickets) > 0 {
		return tickets[0], nil
	}
	return nil, nil
}

const sqlSelectTicketsByID = `
SELECT
  id,
  uuid,
  org_id,
  contact_id,
  status,
  topic_id,
  body,
  assignee_id,
  opened_on,
  opened_by_id,
  opened_in_id,
  replied_on,
  modified_on,
  closed_on,
  last_activity_on
    FROM tickets_ticket
   WHERE id = ANY($1)
ORDER BY opened_on DESC`

// LoadTickets loads all of the tickets with the given ids
func LoadTickets(ctx context.Context, db *sqlx.DB, ids []TicketID) ([]*Ticket, error) {
	return loadTickets(ctx, db, sqlSelectTicketsByID, pq.Array(ids))
}

func loadTickets(ctx context.Context, db *sqlx.DB, query string, params ...any) ([]*Ticket, error) {
	rows, err := db.QueryxContext(ctx, query, params...)
	if err != nil && err != sql.ErrNoRows {
		return nil, errors.Wrap(err, "error loading tickets")
	}
	defer rows.Close()

	tickets := make([]*Ticket, 0, 2)
	for rows.Next() {
		ticket := &Ticket{}
		err = rows.StructScan(&ticket.t)
		if err != nil {
			return nil, errors.Wrapf(err, "error unmarshalling ticket")
		}
		tickets = append(tickets, ticket)
	}

	return tickets, nil
}

const sqlSelectTicketByUUID = `
SELECT
  t.id,
  t.uuid,
  t.org_id,
  t.contact_id,
  t.status,
  t.topic_id,
  t.body,
  t.assignee_id,
  t.opened_on,
  t.opened_by_id,
  t.opened_in_id,
  t.replied_on,
  t.modified_on,
  t.closed_on,
  t.last_activity_on
FROM
  tickets_ticket t
WHERE
  t.uuid = $1`

// LookupTicketByUUID looks up the ticket with the passed in UUID
func LookupTicketByUUID(ctx context.Context, db *sqlx.DB, uuid flows.TicketUUID) (*Ticket, error) {
	return lookupTicket(ctx, db, sqlSelectTicketByUUID, uuid)
}

func lookupTicket(ctx context.Context, db *sqlx.DB, query string, params ...any) (*Ticket, error) {
	rows, err := db.QueryxContext(ctx, query, params...)
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	defer rows.Close()

	if err == sql.ErrNoRows || !rows.Next() {
		return nil, nil
	}

	ticket := &Ticket{}
	err = rows.StructScan(&ticket.t)
	if err != nil {
		return nil, err
	}

	return ticket, nil
}

const sqlInsertTicket = `
INSERT INTO 
  tickets_ticket(uuid,  org_id,  contact_id,  status,  topic_id,  body,  assignee_id,  opened_on, opened_by_id,  opened_in_id,  modified_on, last_activity_on)
  VALUES(        :uuid, :org_id, :contact_id, :status, :topic_id, :body, :assignee_id, NOW(),     :opened_by_id, :opened_in_id, NOW()      , NOW())
RETURNING
  id
`

// InsertTickets inserts the passed in tickets returning any errors encountered
func InsertTickets(ctx context.Context, tx DBorTx, oa *OrgAssets, tickets []*Ticket) error {
	if len(tickets) == 0 {
		return nil
	}

	openingCounts := map[string]int{scopeOrg(oa): len(tickets)} // all new tickets are open
	assignmentCounts := make(map[string]int)

	ts := make([]any, len(tickets))
	for i, t := range tickets {
		ts[i] = &t.t

		if t.AssigneeID() != NilUserID {
			assignee := oa.UserByID(t.AssigneeID())
			if assignee != nil {
				assignmentCounts[scopeUser(oa, assignee)]++
			}
		}
	}

	if err := BulkQuery(ctx, "inserted tickets", tx, sqlInsertTicket, ts); err != nil {
		return err
	}

	if err := insertTicketDailyCounts(ctx, tx, TicketDailyCountOpening, oa.Env().Timezone(), openingCounts); err != nil {
		return err
	}
	if err := insertTicketDailyCounts(ctx, tx, TicketDailyCountAssignment, oa.Env().Timezone(), assignmentCounts); err != nil {
		return err
	}

	return nil
}

// UpdateTicketLastActivity updates the last_activity_on of the given tickets to be now
func UpdateTicketLastActivity(ctx context.Context, db DBorTx, tickets []*Ticket) error {
	now := dates.Now()
	ids := make([]TicketID, len(tickets))
	for i, t := range tickets {
		t.t.LastActivityOn = now
		ids[i] = t.ID()
	}
	return updateTicketLastActivity(ctx, db, ids, now)
}

func updateTicketLastActivity(ctx context.Context, db DBorTx, ids []TicketID, now time.Time) error {
	_, err := db.ExecContext(ctx, `UPDATE tickets_ticket SET last_activity_on = $2 WHERE id = ANY($1)`, pq.Array(ids), now)
	return err
}

const sqlUpdateTicketsAssignment = `
UPDATE tickets_ticket
   SET assignee_id = $2, modified_on = $3, last_activity_on = $3
 WHERE id = ANY($1)`

// TicketsAssign assigns the passed in tickets
func TicketsAssign(ctx context.Context, db DBorTx, oa *OrgAssets, userID UserID, tickets []*Ticket, assigneeID UserID) (map[*Ticket]*TicketEvent, error) {
	ids := make([]TicketID, 0, len(tickets))
	events := make([]*TicketEvent, 0, len(tickets))
	eventsByTicket := make(map[*Ticket]*TicketEvent, len(tickets))
	now := dates.Now()

	assignmentCounts := make(map[string]int)

	for _, ticket := range tickets {
		if ticket.AssigneeID() != assigneeID {

			// if this is an initial assignment record count for user
			if ticket.AssigneeID() == NilUserID && assigneeID != NilUserID {
				assignee := oa.UserByID(assigneeID)
				if assignee != nil {
					assignmentCounts[scopeUser(oa, assignee)]++
				}
			}

			ids = append(ids, ticket.ID())
			t := &ticket.t
			t.AssigneeID = assigneeID
			t.ModifiedOn = now
			t.LastActivityOn = now

			e := NewTicketAssignedEvent(ticket, userID, assigneeID)
			events = append(events, e)
			eventsByTicket[ticket] = e
		}
	}

	// mark the tickets as assigned in the db
	_, err := db.ExecContext(ctx, sqlUpdateTicketsAssignment, pq.Array(ids), assigneeID, now)
	if err != nil {
		return nil, errors.Wrap(err, "error updating tickets")
	}

	err = InsertTicketEvents(ctx, db, events)
	if err != nil {
		return nil, errors.Wrap(err, "error inserting ticket events")
	}

	err = NotificationsFromTicketEvents(ctx, db, oa, eventsByTicket)
	if err != nil {
		return nil, errors.Wrap(err, "error inserting notifications")
	}

	err = insertTicketDailyCounts(ctx, db, TicketDailyCountAssignment, oa.Env().Timezone(), assignmentCounts)
	if err != nil {
		return nil, errors.Wrap(err, "error inserting assignment counts")
	}

	return eventsByTicket, nil
}

// TicketsAddNote adds a note to the passed in tickets
func TicketsAddNote(ctx context.Context, db DBorTx, oa *OrgAssets, userID UserID, tickets []*Ticket, note string) (map[*Ticket]*TicketEvent, error) {
	events := make([]*TicketEvent, 0, len(tickets))
	eventsByTicket := make(map[*Ticket]*TicketEvent, len(tickets))

	for _, ticket := range tickets {
		e := NewTicketNoteAddedEvent(ticket, userID, note)
		events = append(events, e)
		eventsByTicket[ticket] = e
	}

	err := UpdateTicketLastActivity(ctx, db, tickets)
	if err != nil {
		return nil, errors.Wrapf(err, "error updating ticket activity")
	}

	err = InsertTicketEvents(ctx, db, events)
	if err != nil {
		return nil, errors.Wrapf(err, "error inserting ticket events")
	}

	err = NotificationsFromTicketEvents(ctx, db, oa, eventsByTicket)
	if err != nil {
		return nil, errors.Wrap(err, "error inserting notifications")
	}

	return eventsByTicket, nil
}

const sqlUpdateTicketsTopic = `
UPDATE tickets_ticket
   SET topic_id = $2, modified_on = $3, last_activity_on = $3
 WHERE id = ANY($1)`

// TicketsChangeTopic changes the topic of the passed in tickets
func TicketsChangeTopic(ctx context.Context, db DBorTx, oa *OrgAssets, userID UserID, tickets []*Ticket, topicID TopicID) (map[*Ticket]*TicketEvent, error) {
	ids := make([]TicketID, 0, len(tickets))
	events := make([]*TicketEvent, 0, len(tickets))
	eventsByTicket := make(map[*Ticket]*TicketEvent, len(tickets))
	now := dates.Now()

	for _, ticket := range tickets {
		if ticket.TopicID() != topicID {
			ids = append(ids, ticket.ID())
			t := &ticket.t
			t.TopicID = topicID
			t.ModifiedOn = now
			t.LastActivityOn = now

			e := NewTicketTopicChangedEvent(ticket, userID, topicID)
			events = append(events, e)
			eventsByTicket[ticket] = e
		}
	}

	// mark the tickets as assigned in the db
	_, err := db.ExecContext(ctx, sqlUpdateTicketsTopic, pq.Array(ids), topicID, now)
	if err != nil {
		return nil, errors.Wrap(err, "error updating tickets")
	}

	err = InsertTicketEvents(ctx, db, events)
	if err != nil {
		return nil, errors.Wrap(err, "error inserting ticket events")
	}

	return eventsByTicket, nil
}

const sqlCloseTickets = `
UPDATE tickets_ticket
   SET status = 'C', modified_on = $2, closed_on = $2, last_activity_on = $2
 WHERE id = ANY($1)`

// CloseTickets closes the passed in tickets
func CloseTickets(ctx context.Context, rt *runtime.Runtime, oa *OrgAssets, userID UserID, tickets []*Ticket) (map[*Ticket]*TicketEvent, error) {
	ids := make([]TicketID, 0, len(tickets))
	events := make([]*TicketEvent, 0, len(tickets))
	eventsByTicket := make(map[*Ticket]*TicketEvent, len(tickets))
	contactIDs := make(map[ContactID]bool, len(tickets))
	now := dates.Now()

	for _, ticket := range tickets {
		if ticket.Status() != TicketStatusClosed {
			ids = append(ids, ticket.ID())
			t := &ticket.t
			t.Status = TicketStatusClosed
			t.ModifiedOn = now
			t.ClosedOn = &now
			t.LastActivityOn = now

			e := NewTicketClosedEvent(ticket, userID)
			events = append(events, e)
			eventsByTicket[ticket] = e
			contactIDs[ticket.ContactID()] = true
		}
	}

	// mark the tickets as closed in the db
	_, err := rt.DB.ExecContext(ctx, sqlCloseTickets, pq.Array(ids), now)
	if err != nil {
		return nil, errors.Wrap(err, "error updating tickets")
	}

	if err := InsertTicketEvents(ctx, rt.DB, events); err != nil {
		return nil, errors.Wrapf(err, "error inserting ticket events")
	}

	if err := recalcGroupsForTicketChanges(ctx, rt.DB, oa, contactIDs); err != nil {
		return nil, errors.Wrapf(err, "error recalculting groups")
	}

	return eventsByTicket, nil
}

const sqlReopenTickets = `
UPDATE tickets_ticket
   SET status = 'O', modified_on = $2, closed_on = NULL, last_activity_on = $2
 WHERE id = ANY($1)`

// ReopenTickets reopens the passed in tickets
func ReopenTickets(ctx context.Context, rt *runtime.Runtime, oa *OrgAssets, userID UserID, tickets []*Ticket) (map[*Ticket]*TicketEvent, error) {
	ids := make([]TicketID, 0, len(tickets))
	events := make([]*TicketEvent, 0, len(tickets))
	eventsByTicket := make(map[*Ticket]*TicketEvent, len(tickets))
	contactIDs := make(map[ContactID]bool, len(tickets))
	now := dates.Now()

	for _, ticket := range tickets {
		if ticket.Status() != TicketStatusOpen {
			ids = append(ids, ticket.ID())
			t := &ticket.t
			t.Status = TicketStatusOpen
			t.ModifiedOn = now
			t.ClosedOn = nil
			t.LastActivityOn = now

			e := NewTicketReopenedEvent(ticket, userID)
			events = append(events, e)
			eventsByTicket[ticket] = e
			contactIDs[ticket.ContactID()] = true
		}
	}

	// mark the tickets as opened in the db
	_, err := rt.DB.ExecContext(ctx, sqlReopenTickets, pq.Array(ids), now)
	if err != nil {
		return nil, errors.Wrapf(err, "error updating tickets")
	}

	err = InsertTicketEvents(ctx, rt.DB, events)
	if err != nil {
		return nil, errors.Wrapf(err, "error inserting ticket events")
	}

	if err := recalcGroupsForTicketChanges(ctx, rt.DB, oa, contactIDs); err != nil {
		return nil, errors.Wrapf(err, "error recalculting groups")
	}

	return eventsByTicket, nil
}

// because groups can be based on "tickets" need to recalculate after closing/reopening tickets
func recalcGroupsForTicketChanges(ctx context.Context, db DBorTx, oa *OrgAssets, contactIDs map[ContactID]bool) error {
	ids := make([]ContactID, 0, len(contactIDs))
	for cid := range contactIDs {
		ids = append(ids, cid)
	}

	contacts, err := LoadContacts(ctx, db, oa, ids)
	if err != nil {
		return errors.Wrap(err, "error loading contacts with ticket changes")
	}

	flowContacts := make([]*flows.Contact, len(contacts))
	for i, contact := range contacts {
		flowContacts[i], err = contact.FlowContact(oa)
		if err != nil {
			return errors.Wrap(err, "error loading flow contact")
		}
	}

	return CalculateDynamicGroups(ctx, db, oa, flowContacts)
}

const sqlUpdateTicketRepliedOn = `
   UPDATE tickets_ticket t1
      SET last_activity_on = $2, replied_on = LEAST(t1.replied_on, $2)
	 FROM tickets_ticket t2
    WHERE t1.id = t2.id AND t1.id = $1
RETURNING CASE WHEN t2.replied_on IS NULL THEN EXTRACT(EPOCH FROM (t1.replied_on - t1.opened_on)) ELSE NULL END`

// TicketRecordReplied records a ticket as being replied to, updating last_activity_on. If this is the first reply
// to this ticket then replied_on is updated and the function returns the number of seconds between that and when
// the ticket was opened.
func TicketRecordReplied(ctx context.Context, db DBorTx, ticketID TicketID, when time.Time) (time.Duration, error) {
	rows, err := db.QueryxContext(ctx, sqlUpdateTicketRepliedOn, ticketID, when)
	if err != nil && err != sql.ErrNoRows {
		return -1, err
	}

	defer rows.Close()

	// if we didn't get anything back then we didn't change the ticket because it was already replied to
	if err == sql.ErrNoRows || !rows.Next() {
		return -1, nil
	}

	var seconds *float64
	if err := rows.Scan(&seconds); err != nil {
		return -1, err
	}

	if seconds != nil {
		return time.Duration(*seconds * float64(time.Second)), nil
	}

	return time.Duration(-1), nil
}

func insertTicketDailyCounts(ctx context.Context, tx DBorTx, countType TicketDailyCountType, tz *time.Location, scopeCounts map[string]int) error {
	return insertDailyCounts(ctx, tx, "tickets_ticketdailycount", countType, tz, scopeCounts)
}

func insertTicketDailyTiming(ctx context.Context, tx DBorTx, countType TicketDailyTimingType, tz *time.Location, scope string, duration time.Duration) error {
	return insertDailyTiming(ctx, tx, "tickets_ticketdailytiming", countType, tz, scope, duration)
}

func RecordTicketReply(ctx context.Context, db DBorTx, oa *OrgAssets, ticketID TicketID, userID UserID) error {
	firstReplyTime, err := TicketRecordReplied(ctx, db, ticketID, dates.Now())
	if err != nil {
		return err
	}

	// record reply counts for org, user and team
	replyCounts := map[string]int{scopeOrg(oa): 1}

	if userID != NilUserID {
		user := oa.UserByID(userID)
		if user != nil {
			replyCounts[scopeUser(oa, user)] = 1
			if user.Team() != nil {
				replyCounts[scopeTeam(user.Team())] = 1
			}
		}
	}

	if err := insertTicketDailyCounts(ctx, db, TicketDailyCountReply, oa.Env().Timezone(), replyCounts); err != nil {
		return err
	}

	if firstReplyTime >= 0 {
		if err := insertTicketDailyTiming(ctx, db, TicketDailyTimingFirstReply, oa.Env().Timezone(), scopeOrg(oa), firstReplyTime); err != nil {
			return err
		}
	}
	return nil
}
