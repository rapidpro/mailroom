package testdata

import (
	"testing"

	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

type Ticket struct {
	ID   models.TicketID  `db:"id"`
	UUID flows.TicketUUID `db:"uuid"`
}

func (k *Ticket) Load(t *testing.T, db *sqlx.DB) *models.Ticket {
	tickets, err := models.LoadTickets(testsuite.CTX(), db, []models.TicketID{k.ID})
	require.NoError(t, err)
	require.Equal(t, 1, len(tickets))

	return tickets[0]
}

type Ticketer struct {
	ID   models.TicketerID
	UUID assets.TicketerUUID
}

// InsertOpenTicket inserts an open ticket
func InsertOpenTicket(t *testing.T, db *sqlx.DB, org *Org, contact *Contact, ticketer *Ticketer, subject, body, externalID string, assignee *User) *Ticket {
	assigneeID := models.NilUserID
	if assignee != nil {
		assigneeID = assignee.ID
	}

	ticket := &Ticket{}
	err := db.Get(ticket,
		`INSERT INTO tickets_ticket(uuid, org_id, contact_id, ticketer_id, status, subject, body, external_id, opened_on, modified_on, last_activity_on, assignee_id)
		VALUES($1, $2, $3, $4, 'O', $5, $6, $7, NOW(), NOW(), NOW(), $8) RETURNING id, uuid`, uuids.New(), org.ID, contact.ID, ticketer.ID, subject, body, externalID, assigneeID,
	)
	require.NoError(t, err)
	return ticket
}

// InsertClosedTicket inserts a closed ticket
func InsertClosedTicket(t *testing.T, db *sqlx.DB, org *Org, contact *Contact, ticketer *Ticketer, subject, body, externalID string, assignee *User) *Ticket {
	assigneeID := models.NilUserID
	if assignee != nil {
		assigneeID = assignee.ID
	}

	ticket := &Ticket{}
	err := db.Get(ticket,
		`INSERT INTO tickets_ticket(uuid, org_id, contact_id, ticketer_id, status, subject, body, external_id, opened_on, modified_on, closed_on, last_activity_on, assignee_id)
		VALUES($1, $2, $3, $4, 'C', $5, $6, $7, NOW(), NOW(), NOW(), NOW(), $8) RETURNING id, uuid`, uuids.New(), org.ID, contact.ID, ticketer.ID, subject, body, externalID, assigneeID,
	)
	require.NoError(t, err)
	return ticket
}
