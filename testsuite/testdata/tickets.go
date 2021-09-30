package testdata

import (
	"context"
	"time"

	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/models"

	"github.com/jmoiron/sqlx"
)

type Topic struct {
	ID   models.TopicID
	UUID assets.TopicUUID
}

type Ticket struct {
	ID   models.TicketID
	UUID flows.TicketUUID
}

func (k *Ticket) Load(db *sqlx.DB) *models.Ticket {
	tickets, err := models.LoadTickets(context.Background(), db, []models.TicketID{k.ID})
	must(err, len(tickets) == 1)
	return tickets[0]
}

type Ticketer struct {
	ID   models.TicketerID
	UUID assets.TicketerUUID
}

// InsertOpenTicket inserts an open ticket
func InsertOpenTicket(db *sqlx.DB, org *Org, contact *Contact, ticketer *Ticketer, topic *Topic, body, externalID string, assignee *User) *Ticket {
	return insertTicket(db, org, contact, ticketer, models.TicketStatusOpen, topic, body, externalID, assignee)
}

// InsertClosedTicket inserts a closed ticket
func InsertClosedTicket(db *sqlx.DB, org *Org, contact *Contact, ticketer *Ticketer, topic *Topic, body, externalID string, assignee *User) *Ticket {
	return insertTicket(db, org, contact, ticketer, models.TicketStatusClosed, topic, body, externalID, assignee)
}

func insertTicket(db *sqlx.DB, org *Org, contact *Contact, ticketer *Ticketer, status models.TicketStatus, topic *Topic, body, externalID string, assignee *User) *Ticket {
	uuid := flows.TicketUUID(uuids.New())
	var closedOn *time.Time
	if status == models.TicketStatusClosed {
		t := dates.Now()
		closedOn = &t
	}

	var id models.TicketID
	must(db.Get(&id,
		`INSERT INTO tickets_ticket(uuid, org_id, contact_id, ticketer_id, status, topic_id, body, external_id, opened_on, modified_on, closed_on, last_activity_on, assignee_id)
		VALUES($1, $2, $3, $4, $5, $6, $7, $8, NOW(), NOW(), $9, NOW(), $10) RETURNING id`, uuid, org.ID, contact.ID, ticketer.ID, status, topic.ID, body, externalID, closedOn, assignee.SafeID(),
	))
	return &Ticket{id, uuid}
}
