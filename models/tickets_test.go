package models

import (
	"testing"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/testsuite"

	"github.com/stretchr/testify/assert"
)

func TestTickets(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()

	ticket1 := NewTicket(
		"2ef57efc-d85f-4291-b330-e4afe68af5fe",
		Org1,
		CathyID,
		MailgunID,
		"EX12345",
		"New Ticket",
		"Where are my cookies?",
		map[string]interface{}{
			"contact-display": "Cathy",
		},
	)
	ticket2 := NewTicket(
		"64f81be1-00ff-48ef-9e51-97d6f924c1a4",
		Org1,
		BobID,
		ZendeskID,
		"EX7869",
		"New Zen Ticket",
		"Where are my trousers?",
		nil,
	)

	assert.Equal(t, flows.TicketUUID("2ef57efc-d85f-4291-b330-e4afe68af5fe"), ticket1.UUID())

	err := InsertTickets(ctx, db, []*Ticket{ticket1, ticket2})
	assert.NoError(t, err)

	// check both tickets were created
	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM tickets_ticket WHERE org_id = $1 AND status = 'O' AND closed_on IS NULL`, []interface{}{Org1}, 2)

	// can lookup a ticket by UUID
	tk, err := LookupTicketByUUID(ctx, db, "2ef57efc-d85f-4291-b330-e4afe68af5fe")
	assert.NoError(t, err)
	assert.Equal(t, "New Ticket", tk.Subject())

	err = UpdateAndKeepOpenTicket(ctx, db, ticket1, map[string]string{"last-message-id": "2352"})
	assert.NoError(t, err)

	// check ticket remains open and config was updated
	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM tickets_ticket WHERE org_id = $1 AND status = 'O' AND config='{"contact-display": "Cathy", "last-message-id": "2352"}'::jsonb AND closed_on IS NULL`, []interface{}{Org1}, 1)

	err = CloseTicket(ctx, db, ticket1)
	assert.NoError(t, err)

	// check ticket is now closed
	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM tickets_ticket WHERE org_id = $1 AND status = 'C' AND closed_on IS NOT NULL`, []interface{}{Org1}, 1)

	err = UpdateAndKeepOpenTicket(ctx, db, ticket1, map[string]string{"last-message-id": "6754"})
	assert.NoError(t, err)

	// check ticket is open again
	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM tickets_ticket WHERE org_id = $1 AND status = 'O' AND closed_on IS NULL`, []interface{}{Org1}, 2)
}
