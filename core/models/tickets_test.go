package models_test

import (
	"testing"

	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/models"
	_ "github.com/nyaruka/mailroom/services/tickets/mailgun"
	_ "github.com/nyaruka/mailroom/services/tickets/zendesk"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/nyaruka/null"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTicketers(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()

	// can load directly by UUID
	ticketer, err := models.LookupTicketerByUUID(ctx, db, testdata.Zendesk.UUID)
	assert.NoError(t, err)
	assert.Equal(t, testdata.Zendesk.ID, ticketer.ID())
	assert.Equal(t, testdata.Zendesk.UUID, ticketer.UUID())
	assert.Equal(t, "Zendesk (Nyaruka)", ticketer.Name())
	assert.Equal(t, "1234-abcd", ticketer.Config("push_id"))
	assert.Equal(t, "523562", ticketer.Config("push_token"))

	// org through org assets
	org1, err := models.GetOrgAssets(ctx, db, testdata.Org1.ID)
	assert.NoError(t, err)

	ticketer = org1.TicketerByID(testdata.Zendesk.ID)
	assert.Equal(t, testdata.Zendesk.UUID, ticketer.UUID())
	assert.Equal(t, "Zendesk (Nyaruka)", ticketer.Name())
	assert.Equal(t, "1234-abcd", ticketer.Config("push_id"))

	ticketer = org1.TicketerByUUID(testdata.Zendesk.UUID)
	assert.Equal(t, testdata.Zendesk.UUID, ticketer.UUID())
	assert.Equal(t, "Zendesk (Nyaruka)", ticketer.Name())
	assert.Equal(t, "1234-abcd", ticketer.Config("push_id"))

	ticketer.UpdateConfig(ctx, db, map[string]string{"new-key": "foo"}, map[string]bool{"push_id": true})

	org1, _ = models.GetOrgAssetsWithRefresh(ctx, db, testdata.Org1.ID, models.RefreshTicketers)
	ticketer = org1.TicketerByID(testdata.Zendesk.ID)

	assert.Equal(t, "foo", ticketer.Config("new-key"))       // new config value added
	assert.Equal(t, "", ticketer.Config("push_id"))          // existing config value removed
	assert.Equal(t, "523562", ticketer.Config("push_token")) // other value unchanged
}

func TestTickets(t *testing.T) {
	ctx := testsuite.CTX()
	rt := testsuite.RT()
	db := rt.DB

	defer httpx.SetRequestor(httpx.DefaultRequestor)

	httpx.SetRequestor(httpx.NewMockRequestor(map[string][]httpx.MockResponse{
		"https://api.mailgun.net/v3/tickets.rapidpro.io/messages": {
			httpx.NewMockResponse(200, nil, `{
				"id": "<20200426161758.1.590432020254B2BF@tickets.rapidpro.io>",
				"message": "Queued. Thank you."
			}`),
		},
	}))

	ticket1 := models.NewTicket(
		"2ef57efc-d85f-4291-b330-e4afe68af5fe",
		testdata.Org1.ID,
		testdata.Cathy.ID,
		testdata.Mailgun.ID,
		"EX12345",
		"New Ticket",
		"Where are my cookies?",
		map[string]interface{}{
			"contact-display": "Cathy",
		},
	)
	ticket2 := models.NewTicket(
		"64f81be1-00ff-48ef-9e51-97d6f924c1a4",
		testdata.Org1.ID,
		testdata.Bob.ID,
		testdata.Zendesk.ID,
		"EX7869",
		"New Zen Ticket",
		"Where are my trousers?",
		nil,
	)
	ticket3 := models.NewTicket(
		"28ef8ddc-b221-42f3-aeae-ee406fc9d716",
		testdata.Org2.ID,
		testdata.Alexandria.ID,
		testdata.Zendesk.ID,
		"EX6677",
		"Other Org Ticket",
		"Where are my pants?",
		nil,
	)

	assert.Equal(t, flows.TicketUUID("2ef57efc-d85f-4291-b330-e4afe68af5fe"), ticket1.UUID())
	assert.Equal(t, testdata.Org1.ID, ticket1.OrgID())
	assert.Equal(t, testdata.Cathy.ID, ticket1.ContactID())
	assert.Equal(t, testdata.Mailgun.ID, ticket1.TicketerID())
	assert.Equal(t, null.String("EX12345"), ticket1.ExternalID())
	assert.Equal(t, "New Ticket", ticket1.Subject())
	assert.Equal(t, "Cathy", ticket1.Config("contact-display"))
	assert.Equal(t, "", ticket1.Config("xyz"))

	err := models.InsertTickets(ctx, db, []*models.Ticket{ticket1, ticket2, ticket3})
	assert.NoError(t, err)

	// check all tickets were created
	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM tickets_ticket WHERE status = 'O' AND closed_on IS NULL`, nil, 3)

	// can lookup a ticket by UUID
	tk1, err := models.LookupTicketByUUID(ctx, db, "2ef57efc-d85f-4291-b330-e4afe68af5fe")
	assert.NoError(t, err)
	assert.Equal(t, "New Ticket", tk1.Subject())

	// can lookup a ticket by external ID and ticketer
	tk2, err := models.LookupTicketByExternalID(ctx, db, testdata.Zendesk.ID, "EX7869")
	assert.NoError(t, err)
	assert.Equal(t, "New Zen Ticket", tk2.Subject())

	// can lookup open tickets by contact
	org1, _ := models.GetOrgAssets(ctx, db, testdata.Org1.ID)
	cathy, err := models.LoadContact(ctx, db, org1, testdata.Cathy.ID)
	require.NoError(t, err)

	tks, err := models.LoadOpenTicketsForContact(ctx, db, cathy)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tks))
	assert.Equal(t, "New Ticket", tks[0].Subject())

	err = models.UpdateAndKeepOpenTicket(ctx, db, ticket1, map[string]string{"last-message-id": "2352"})
	assert.NoError(t, err)

	// check ticket remains open and config was updated
	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM tickets_ticket WHERE org_id = $1 AND status = 'O' AND config='{"contact-display": "Cathy", "last-message-id": "2352"}'::jsonb AND closed_on IS NULL`, []interface{}{testdata.Org1.ID}, 1)

	logger := &models.HTTPLogger{}

	err = models.CloseTickets(ctx, db, org1, []*models.Ticket{ticket1}, true, logger)
	assert.NoError(t, err)

	// check ticket is now closed
	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM tickets_ticket WHERE org_id = $1 AND status = 'C' AND closed_on IS NOT NULL`, []interface{}{testdata.Org1.ID}, 1)

	err = models.UpdateAndKeepOpenTicket(ctx, db, ticket1, map[string]string{"last-message-id": "6754"})
	assert.NoError(t, err)

	// check ticket is open again
	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM tickets_ticket WHERE org_id = $1 AND status = 'O' AND closed_on IS NULL`, []interface{}{testdata.Org1.ID}, 2)
}
