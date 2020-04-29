package hooks

import (
	"testing"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/actions"
	"github.com/nyaruka/goflow/utils/httpx"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/testsuite"
)

func TestTicketOpened(t *testing.T) {
	testsuite.Reset()

	defer httpx.SetRequestor(httpx.DefaultRequestor)

	httpx.SetRequestor(httpx.NewMockRequestor(map[string][]httpx.MockResponse{
		"https://api.mailgun.net/v3/mr.nyaruka.com/messages": {
			httpx.NewMockResponse(200, nil, `{
				"id": "<20200426161758.1.590432020254B2BF@mr.nyaruka.com>",
				"message": "Queued. Thank you."
			}`),
		},
	}))

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
				{
					SQL:   "select count(*) from tickets_ticket where contact_id = $1 AND status = 'O' AND ticketer_id = $2",
					Args:  []interface{}{models.CathyID, models.MailgunID},
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
