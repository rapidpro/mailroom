package hooks

import (
	"testing"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/actions"
	"github.com/nyaruka/goflow/utils/httpx"
	"github.com/nyaruka/goflow/utils/uuids"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/testsuite"

	_ "github.com/nyaruka/mailroom/services/ticket/mailgun"
	_ "github.com/nyaruka/mailroom/services/ticket/zendesk"

	"github.com/stretchr/testify/require"
)

func TestTicketOpened(t *testing.T) {
	testsuite.Reset()
	db := testsuite.DB()
	ctx := testsuite.CTX()

	defer httpx.SetRequestor(httpx.DefaultRequestor)

	httpx.SetRequestor(httpx.NewMockRequestor(map[string][]httpx.MockResponse{
		"https://api.mailgun.net/v3/mr.nyaruka.com/messages": {
			httpx.NewMockResponse(200, nil, `{
				"id": "<20200426161758.1.590432020254B2BF@mr.nyaruka.com>",
				"message": "Queued. Thank you."
			}`),
		},
		"https://nyaruka.zendesk.com/api/v2/tickets.json": {
			httpx.NewMockResponse(200, nil, `{
				"ticket":{
					"id": 12345,
					"url": "https://nyaruka.zendesk.com/api/v2/tickets/12345.json",
					"external_id": "a78c5d9d-283a-4be9-ad6d-690e4307c961",
					"created_at": "2009-07-20T22:55:29Z",
					"subject": "Need help"
				}
			}`),
		},
	}))

	// an existing ticket
	cathyClosedTicket := models.NewTicket(flows.TicketUUID(uuids.New()), models.Org1, models.CathyID, models.MailgunID, "748363", "Old Question", "Who?", nil)
	err := models.InsertTickets(ctx, db, []*models.Ticket{cathyClosedTicket})
	require.NoError(t, err)

	tcs := []HookTestCase{
		{
			Actions: ContactActionMap{
				models.CathyID: []flows.Action{
					actions.NewOpenTicket(newActionUUID(), assets.NewTicketerReference(models.MailgunUUID, "Mailgun (IT Support)"), "Need help", "Where are my cookies?", "Email Ticket"),
				},
				models.BobID: []flows.Action{
					actions.NewOpenTicket(newActionUUID(), assets.NewTicketerReference(models.ZendeskUUID, "Zendesk (Nyaruka)"), "Interesting", "I've found some cookies", "Zen Ticket"),
				},
			},
			SQLAssertions: []SQLAssertion{
				{ // cathy's old ticket will still be open and cathy's new ticket will have been created
					SQL:   "select count(*) from tickets_ticket where contact_id = $1 AND status = 'O' AND ticketer_id = $2",
					Args:  []interface{}{models.CathyID, models.MailgunID},
					Count: 2,
				},
				{ // and there's an HTTP log for that
					SQL:   "select count(*) from request_logs_httplog where ticketer_id = $1",
					Args:  []interface{}{models.MailgunID},
					Count: 1,
				},
				{
					SQL:   "select count(*) from tickets_ticket where contact_id = $1 AND status = 'O' AND ticketer_id = $2",
					Args:  []interface{}{models.BobID, models.ZendeskID},
					Count: 1,
				},
			},
		},
	}

	RunActionTestCases(t, tcs)
}
