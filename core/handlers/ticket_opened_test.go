package handlers_test

import (
	"testing"

	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/actions"
	"github.com/nyaruka/mailroom/core/handlers"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
)

func TestTicketOpened(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetAll)
	defer httpx.SetRequestor(httpx.DefaultRequestor)

	httpx.SetRequestor(httpx.NewMockRequestor(map[string][]*httpx.MockResponse{
		"https://api.mailgun.net/v3/tickets.rapidpro.io/messages": {
			httpx.NewMockResponse(200, nil, []byte(`{
				"id": "<20200426161758.1.590432020254B2BF@tickets.rapidpro.io>",
				"message": "Queued. Thank you."
			}`)),
		},
		"https://nyaruka.zendesk.com/api/v2/any_channel/push.json": {
			httpx.NewMockResponse(201, nil, []byte(`{
				"results": [
					{
						"external_resource_id": "123",
						"status": {"code": "success"}
					}
				]
			}`)),
		},
	}))

	tcs := []handlers.TestCase{
		{
			Actions: handlers.ContactActionMap{
				testdata.Cathy: []flows.Action{
					actions.NewOpenTicket(
						handlers.NewActionUUID(),
						assets.NewTopicReference(testdata.SupportTopic.UUID, "Support"),
						"Where are my cookies?",
						assets.NewUserReference(testdata.Admin.Email, "Admin"),
						"Email Ticket",
					),
				},
				testdata.Bob: []flows.Action{
					actions.NewOpenTicket(
						handlers.NewActionUUID(),
						nil,
						"I've found some cookies",
						nil,
						"Zen Ticket",
					),
				},
			},
			SQLAssertions: []handlers.SQLAssertion{
				{ // cathy's old ticket will still be open and cathy's new ticket will have been created
					SQL:   "select count(*) from tickets_ticket where contact_id = $1 AND status = 'O'",
					Args:  []any{testdata.Cathy.ID},
					Count: 1,
				},
				{ // bob's ticket will have been created too
					SQL:   "select count(*) from tickets_ticket where contact_id = $1 AND status = 'O'",
					Args:  []any{testdata.Bob.ID},
					Count: 1,
				},
				{ // and we have 2 ticket opened events for the 2 tickets opened
					SQL:   "select count(*) from tickets_ticketevent where event_type = 'O'",
					Count: 2,
				},
				{ // both of our tickets have a topic (one without an explicit topic get's the default)
					SQL:   "select count(*) from tickets_ticket where topic_id is null",
					Count: 0,
				},
				{ // one of our tickets is assigned to admin
					SQL:   "select count(*) from tickets_ticket where assignee_id = $1",
					Args:  []any{testdata.Admin.ID},
					Count: 1,
				},
				{ // admin will have a ticket assigned notification for the ticket directly assigned to them
					SQL:   "select count(*) from notifications_notification where user_id = $1 and notification_type = 'tickets:activity'",
					Args:  []any{testdata.Admin.ID},
					Count: 1,
				},
				{ // all assignable users will have a ticket opened notification for the unassigned ticket
					SQL:   "select count(*) from notifications_notification where notification_type = 'tickets:opened'",
					Args:  nil,
					Count: 3,
				},
			},
		},
	}

	handlers.RunTestCases(t, ctx, rt, tcs)
}
