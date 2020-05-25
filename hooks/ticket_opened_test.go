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

	_ "github.com/nyaruka/mailroom/services/tickets/mailgun"
	_ "github.com/nyaruka/mailroom/services/tickets/zendesk"

	"github.com/stretchr/testify/require"
)

func TestTicketOpened(t *testing.T) {
	testsuite.Reset()
	db := testsuite.DB()
	ctx := testsuite.CTX()

	defer httpx.SetRequestor(httpx.DefaultRequestor)

	httpx.SetRequestor(httpx.NewMockRequestor(map[string][]httpx.MockResponse{
		"https://api.mailgun.net/v3/tickets.rapidpro.io/messages": {
			httpx.NewMockResponse(200, nil, `{
				"id": "<20200426161758.1.590432020254B2BF@tickets.rapidpro.io>",
				"message": "Queued. Thank you."
			}`),
		},
		"https://nyaruka.zendesk.com/api/v2/any_channel/push.json": {
			httpx.NewMockResponse(201, nil, `{
				"results": [
					{
						"external_resource_id": "123",
						"status": {"code": "success"}
					}
				]
			}`),
		},
	}))

	// an existing ticket
	cathyTicket := models.NewTicket(flows.TicketUUID(uuids.New()), models.Org1, models.CathyID, models.MailgunID, "748363", "Old Question", "Who?", nil)
	err := models.InsertTickets(ctx, db, []*models.Ticket{cathyTicket})
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
				{ // which doesn't include our API token
					SQL:   "select count(*) from request_logs_httplog where ticketer_id = $1 AND request like '%sesame%'",
					Args:  []interface{}{models.MailgunID},
					Count: 0,
				},
				{ // bob's ticket will have been created too
					SQL:   "select count(*) from tickets_ticket where contact_id = $1 AND status = 'O' AND ticketer_id = $2",
					Args:  []interface{}{models.BobID, models.ZendeskID},
					Count: 1,
				},
				{ // and there's an HTTP log for that
					SQL:   "select count(*) from request_logs_httplog where ticketer_id = $1",
					Args:  []interface{}{models.ZendeskID},
					Count: 1,
				},
				{ // which doesn't include our API token
					SQL:   "select count(*) from request_logs_httplog where ticketer_id = $1 AND request like '%523562%'",
					Args:  []interface{}{models.ZendeskID},
					Count: 0,
				},
			},
		},
	}

	RunHookTestCases(t, tcs)
}
