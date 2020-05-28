package tickets_test

import (
	"testing"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/services/tickets"
	_ "github.com/nyaruka/mailroom/services/tickets/mailgun"
	_ "github.com/nyaruka/mailroom/services/tickets/zendesk"
	"github.com/nyaruka/mailroom/testsuite"

	"github.com/stretchr/testify/assert"
)

func TestFromTicketUUID(t *testing.T) {
	testsuite.ResetDB()
	ctx := testsuite.CTX()
	db := testsuite.DB()

	ticket1UUID := flows.TicketUUID("f7358870-c3dd-450d-b5ae-db2eb50216ba")
	ticket2UUID := flows.TicketUUID("44b7d9b5-6ddd-4a6a-a1c0-8b70ecd06339")

	// create some tickets
	db.MustExec(`INSERT INTO tickets_ticket(id, uuid,  org_id, contact_id, ticketer_id, status, subject, body, opened_on, modified_on)
	VALUES(1, $1, $2, $3, $4, 'O', 'Need help', 'Have you seen my cookies?', NOW(), NOW())`, ticket1UUID, models.Org1, models.CathyID, models.MailgunID)

	db.MustExec(`INSERT INTO tickets_ticket(id, uuid,  org_id, contact_id, ticketer_id, status, subject, body, opened_on, modified_on)
	VALUES(2, $1, $2, $3, $4, 'O', 'Need help', 'Have you seen my shoes?', NOW(), NOW())`, ticket2UUID, models.Org1, models.CathyID, models.ZendeskID)

	// break mailgun configuration
	db.MustExec(`UPDATE tickets_ticketer SET config = '{"foo":"bar"}'::jsonb WHERE id = $1`, models.MailgunID)

	// err if no ticket with UUID
	_, _, _, err := tickets.FromTicketUUID(ctx, db, "33c54d0c-bd49-4edf-87a9-c391a75a630c", "mailgun")
	assert.EqualError(t, err, "error looking up ticket 33c54d0c-bd49-4edf-87a9-c391a75a630c")

	// err if no ticketer type doesn't match
	_, _, _, err = tickets.FromTicketUUID(ctx, db, ticket1UUID, "zendesk")
	assert.EqualError(t, err, "error looking up ticketer #1")

	// err if ticketer isn't configured correctly and can't be loaded as a service
	_, _, _, err = tickets.FromTicketUUID(ctx, db, ticket1UUID, "mailgun")
	assert.EqualError(t, err, "error loading ticketer service: missing domain or api_key or to_address or url_base in mailgun config")

	// if all is correct, returns the ticket, ticketer asset, and ticket service
	ticket, ticketer, svc, err := tickets.FromTicketUUID(ctx, db, ticket2UUID, "zendesk")

	assert.Equal(t, ticket2UUID, ticket.UUID())
	assert.Equal(t, models.ZendeskUUID, ticketer.UUID())
	assert.Implements(t, (*models.TicketService)(nil), svc)
}
