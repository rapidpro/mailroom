package models_test

import (
	"testing"
	"time"

	"github.com/nyaruka/gocommon/dates"
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
	ctx, rt, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetAll)

	// can load directly by UUID
	ticketer, err := models.LookupTicketerByUUID(ctx, db, testdata.Zendesk.UUID)
	assert.NoError(t, err)
	assert.Equal(t, testdata.Zendesk.ID, ticketer.ID())
	assert.Equal(t, testdata.Zendesk.UUID, ticketer.UUID())
	assert.Equal(t, "Zendesk (Nyaruka)", ticketer.Name())
	assert.Equal(t, "1234-abcd", ticketer.Config("push_id"))
	assert.Equal(t, "523562", ticketer.Config("push_token"))

	// org through org assets
	org1, err := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
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

	org1, _ = models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshTicketers)
	ticketer = org1.TicketerByID(testdata.Zendesk.ID)

	assert.Equal(t, "foo", ticketer.Config("new-key"))       // new config value added
	assert.Equal(t, "", ticketer.Config("push_id"))          // existing config value removed
	assert.Equal(t, "523562", ticketer.Config("push_token")) // other value unchanged
}

func TestTickets(t *testing.T) {
	ctx, rt, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetData)

	ticket1 := models.NewTicket(
		"2ef57efc-d85f-4291-b330-e4afe68af5fe",
		testdata.Org1.ID,
		testdata.Cathy.ID,
		testdata.Mailgun.ID,
		"EX12345",
		testdata.DefaultTopic.ID,
		"Where are my cookies?",
		testdata.Admin.ID,
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
		testdata.SalesTopic.ID,
		"Where are my trousers?",
		models.NilUserID,
		nil,
	)
	ticket3 := models.NewTicket(
		"28ef8ddc-b221-42f3-aeae-ee406fc9d716",
		testdata.Org2.ID,
		testdata.Alexandria.ID,
		testdata.Zendesk.ID,
		"EX6677",
		testdata.SupportTopic.ID,
		"Where are my pants?",
		testdata.Org2Admin.ID,
		nil,
	)

	assert.Equal(t, flows.TicketUUID("2ef57efc-d85f-4291-b330-e4afe68af5fe"), ticket1.UUID())
	assert.Equal(t, testdata.Org1.ID, ticket1.OrgID())
	assert.Equal(t, testdata.Cathy.ID, ticket1.ContactID())
	assert.Equal(t, testdata.Mailgun.ID, ticket1.TicketerID())
	assert.Equal(t, null.String("EX12345"), ticket1.ExternalID())
	assert.Equal(t, testdata.DefaultTopic.ID, ticket1.TopicID())
	assert.Equal(t, "Cathy", ticket1.Config("contact-display"))
	assert.Equal(t, testdata.Admin.ID, ticket1.AssigneeID())
	assert.Equal(t, "", ticket1.Config("xyz"))

	err := models.InsertTickets(ctx, db, []*models.Ticket{ticket1, ticket2, ticket3})
	assert.NoError(t, err)

	// check all tickets were created
	testsuite.AssertQuery(t, db, `SELECT count(*) FROM tickets_ticket WHERE status = 'O' AND closed_on IS NULL`).Returns(3)

	// can lookup a ticket by UUID
	tk1, err := models.LookupTicketByUUID(ctx, db, "2ef57efc-d85f-4291-b330-e4afe68af5fe")
	assert.NoError(t, err)
	assert.Equal(t, "Where are my cookies?", tk1.Body())

	// can lookup a ticket by external ID and ticketer
	tk2, err := models.LookupTicketByExternalID(ctx, db, testdata.Zendesk.ID, "EX7869")
	assert.NoError(t, err)
	assert.Equal(t, "Where are my trousers?", tk2.Body())

	// can lookup open tickets by contact
	org1, _ := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
	cathy, err := models.LoadContact(ctx, db, org1, testdata.Cathy.ID)
	require.NoError(t, err)

	tks, err := models.LoadOpenTicketsForContact(ctx, db, cathy)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tks))
	assert.Equal(t, "Where are my cookies?", tks[0].Body())
}

func TestUpdateTicketConfig(t *testing.T) {
	ctx, _, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetData)

	ticket := testdata.InsertOpenTicket(db, testdata.Org1, testdata.Cathy, testdata.Mailgun, testdata.DefaultTopic, "Where my shoes", "123", nil)
	modelTicket := ticket.Load(db)

	// empty configs are null
	testsuite.AssertQuery(t, db, `SELECT count(*) FROM tickets_ticket WHERE config IS NULL AND id = $1`, ticket.ID).Returns(1)

	models.UpdateTicketConfig(ctx, db, modelTicket, map[string]string{"foo": "2352", "bar": "abc"})

	testsuite.AssertQuery(t, db, `SELECT count(*) FROM tickets_ticket WHERE config='{"foo": "2352", "bar": "abc"}'::jsonb AND id = $1`, ticket.ID).Returns(1)

	// updates are additive
	models.UpdateTicketConfig(ctx, db, modelTicket, map[string]string{"foo": "6547", "zed": "xyz"})

	testsuite.AssertQuery(t, db, `SELECT count(*) FROM tickets_ticket WHERE config='{"foo": "6547", "bar": "abc", "zed": "xyz"}'::jsonb AND id = $1`, ticket.ID).Returns(1)
}

func TestUpdateTicketLastActivity(t *testing.T) {
	ctx, _, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetData)

	now := time.Date(2021, 6, 22, 15, 59, 30, 123456789, time.UTC)

	defer dates.SetNowSource(dates.DefaultNowSource)
	dates.SetNowSource(dates.NewFixedNowSource(now))

	ticket := testdata.InsertOpenTicket(db, testdata.Org1, testdata.Cathy, testdata.Mailgun, testdata.DefaultTopic, "Where my shoes", "123", nil)
	modelTicket := ticket.Load(db)

	models.UpdateTicketLastActivity(ctx, db, []*models.Ticket{modelTicket})

	assert.Equal(t, now, modelTicket.LastActivityOn())

	testsuite.AssertQuery(t, db, `SELECT count(*) FROM tickets_ticket WHERE id = $1 AND last_activity_on = $2`, ticket.ID, modelTicket.LastActivityOn()).Returns(1)

}

func TestTicketsAssign(t *testing.T) {
	ctx, rt, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetData)

	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshTicketers)
	require.NoError(t, err)

	ticket1 := testdata.InsertClosedTicket(db, testdata.Org1, testdata.Cathy, testdata.Mailgun, testdata.DefaultTopic, "Where my shoes", "123", nil)
	modelTicket1 := ticket1.Load(db)

	ticket2 := testdata.InsertOpenTicket(db, testdata.Org1, testdata.Cathy, testdata.Zendesk, testdata.DefaultTopic, "Where my pants", "234", nil)
	modelTicket2 := ticket2.Load(db)

	testdata.InsertOpenTicket(db, testdata.Org1, testdata.Cathy, testdata.Mailgun, testdata.DefaultTopic, "", "", nil)

	evts, err := models.TicketsAssign(ctx, db, oa, testdata.Admin.ID, []*models.Ticket{modelTicket1, modelTicket2}, testdata.Agent.ID, "please handle these")
	require.NoError(t, err)
	assert.Equal(t, 2, len(evts))
	assert.Equal(t, models.TicketEventTypeAssigned, evts[modelTicket1].EventType())
	assert.Equal(t, models.TicketEventTypeAssigned, evts[modelTicket2].EventType())

	// check tickets are now assigned
	testsuite.AssertQuery(t, db, `SELECT assignee_id FROM tickets_ticket WHERE id = $1`, ticket1.ID).Columns(map[string]interface{}{"assignee_id": int64(testdata.Agent.ID)})
	testsuite.AssertQuery(t, db, `SELECT assignee_id FROM tickets_ticket WHERE id = $1`, ticket2.ID).Columns(map[string]interface{}{"assignee_id": int64(testdata.Agent.ID)})

	// and there are new assigned events
	testsuite.AssertQuery(t, db, `SELECT count(*) FROM tickets_ticketevent WHERE event_type = 'A' AND note = 'please handle these'`).Returns(2)

	testsuite.AssertQuery(t, db, `SELECT count(*) FROM notifications_notification WHERE user_id = $1 AND notification_type = 'tickets:activity'`, testdata.Agent.ID).Returns(1)
}

func TestTicketsAddNote(t *testing.T) {
	ctx, rt, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetData)

	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshTicketers)
	require.NoError(t, err)

	ticket1 := testdata.InsertClosedTicket(db, testdata.Org1, testdata.Cathy, testdata.Mailgun, testdata.DefaultTopic, "Where my shoes", "123", nil)
	modelTicket1 := ticket1.Load(db)

	ticket2 := testdata.InsertOpenTicket(db, testdata.Org1, testdata.Cathy, testdata.Zendesk, testdata.DefaultTopic, "Where my pants", "234", testdata.Agent)
	modelTicket2 := ticket2.Load(db)

	testdata.InsertOpenTicket(db, testdata.Org1, testdata.Cathy, testdata.Mailgun, testdata.DefaultTopic, "", "", nil)

	evts, err := models.TicketsAddNote(ctx, db, oa, testdata.Admin.ID, []*models.Ticket{modelTicket1, modelTicket2}, "spam")
	require.NoError(t, err)
	assert.Equal(t, 2, len(evts))
	assert.Equal(t, models.TicketEventTypeNoteAdded, evts[modelTicket1].EventType())
	assert.Equal(t, models.TicketEventTypeNoteAdded, evts[modelTicket2].EventType())

	// check there are new note events
	testsuite.AssertQuery(t, db, `SELECT count(*) FROM tickets_ticketevent WHERE event_type = 'N' AND note = 'spam'`).Returns(2)

	testsuite.AssertQuery(t, db, `SELECT count(*) FROM notifications_notification WHERE user_id = $1 AND notification_type = 'tickets:activity'`, testdata.Agent.ID).Returns(1)
}

func TestTicketsChangeTopic(t *testing.T) {
	ctx, rt, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetData)

	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshTicketers)
	require.NoError(t, err)

	ticket1 := testdata.InsertClosedTicket(db, testdata.Org1, testdata.Cathy, testdata.Mailgun, testdata.SalesTopic, "Where my shoes", "123", nil)
	modelTicket1 := ticket1.Load(db)

	ticket2 := testdata.InsertOpenTicket(db, testdata.Org1, testdata.Cathy, testdata.Zendesk, testdata.SupportTopic, "Where my pants", "234", nil)
	modelTicket2 := ticket2.Load(db)

	ticket3 := testdata.InsertOpenTicket(db, testdata.Org1, testdata.Cathy, testdata.Zendesk, testdata.DefaultTopic, "Where my pants", "345", nil)
	modelTicket3 := ticket3.Load(db)

	testdata.InsertOpenTicket(db, testdata.Org1, testdata.Cathy, testdata.Mailgun, testdata.DefaultTopic, "", "", nil)

	evts, err := models.TicketsChangeTopic(ctx, db, oa, testdata.Admin.ID, []*models.Ticket{modelTicket1, modelTicket2, modelTicket3}, testdata.SupportTopic.ID)
	require.NoError(t, err)
	assert.Equal(t, 2, len(evts)) // ticket 2 not included as already has that topic
	assert.Equal(t, models.TicketEventTypeTopicChanged, evts[modelTicket1].EventType())
	assert.Equal(t, models.TicketEventTypeTopicChanged, evts[modelTicket3].EventType())

	// check tickets are updated and we have events
	testsuite.AssertQuery(t, db, `SELECT count(*) FROM tickets_ticket WHERE topic_id = $1`, testdata.SupportTopic.ID).Returns(3)
	testsuite.AssertQuery(t, db, `SELECT count(*) FROM tickets_ticketevent WHERE event_type = 'T' AND topic_id = $1`, testdata.SupportTopic.ID).Returns(2)
}

func TestCloseTickets(t *testing.T) {
	ctx, rt, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetData)
	defer httpx.SetRequestor(httpx.DefaultRequestor)

	httpx.SetRequestor(httpx.NewMockRequestor(map[string][]httpx.MockResponse{
		"https://api.mailgun.net/v3/tickets.rapidpro.io/messages": {
			httpx.NewMockResponse(200, nil, `{
				"id": "<20200426161758.1.590432020254B2BF@tickets.rapidpro.io>",
				"message": "Queued. Thank you."
			}`),
		},
	}))

	testdata.InsertContactGroup(db, testdata.Org1, "94c816d7-cc87-42db-a577-ce072ceaab80", "Tickets", "tickets > 0")

	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshTicketers|models.RefreshGroups)
	require.NoError(t, err)

	ticket1 := testdata.InsertOpenTicket(db, testdata.Org1, testdata.Cathy, testdata.Mailgun, testdata.DefaultTopic, "Where my shoes", "123", nil)
	modelTicket1 := ticket1.Load(db)

	ticket2 := testdata.InsertClosedTicket(db, testdata.Org1, testdata.Cathy, testdata.Zendesk, testdata.DefaultTopic, "Where my pants", "234", nil)
	modelTicket2 := ticket2.Load(db)

	_, cathy := testdata.Cathy.Load(db, oa)

	err = models.CalculateDynamicGroups(ctx, db, oa, []*flows.Contact{cathy})
	require.NoError(t, err)

	assert.Equal(t, "Doctors", cathy.Groups().All()[0].Name())
	assert.Equal(t, "Tickets", cathy.Groups().All()[1].Name())

	logger := &models.HTTPLogger{}
	evts, err := models.CloseTickets(ctx, rt, oa, testdata.Admin.ID, []*models.Ticket{modelTicket1, modelTicket2}, true, false, logger)
	require.NoError(t, err)
	assert.Equal(t, 1, len(evts))
	assert.Equal(t, models.TicketEventTypeClosed, evts[modelTicket1].EventType())

	// check ticket #1 is now closed
	testsuite.AssertQuery(t, db, `SELECT count(*) FROM tickets_ticket WHERE id = $1 AND status = 'C' AND closed_on IS NOT NULL`, ticket1.ID).Returns(1)

	// and there's closed event for it
	testsuite.AssertQuery(t, db, `SELECT count(*) FROM tickets_ticketevent WHERE org_id = $1 AND ticket_id = $2 AND event_type = 'C'`,
		[]interface{}{testdata.Org1.ID, ticket1.ID}, 1)

	// and the logger has an http log it can insert for that ticketer
	require.NoError(t, logger.Insert(ctx, db))

	testsuite.AssertQuery(t, db, `SELECT count(*) FROM request_logs_httplog WHERE ticketer_id = $1`, testdata.Mailgun.ID).Returns(1)

	// reload Cathy and check they're no longer in the tickets group
	_, cathy = testdata.Cathy.Load(db, oa)
	assert.Equal(t, 1, len(cathy.Groups().All()))
	assert.Equal(t, "Doctors", cathy.Groups().All()[0].Name())

	// but no events for ticket #2 which was already closed
	testsuite.AssertQuery(t, db, `SELECT count(*) FROM tickets_ticketevent WHERE ticket_id = $1 AND event_type = 'C'`, ticket2.ID).Returns(0)

	// can close tickets without a user
	ticket3 := testdata.InsertOpenTicket(db, testdata.Org1, testdata.Cathy, testdata.Mailgun, testdata.DefaultTopic, "Where my shoes", "123", nil)
	modelTicket3 := ticket3.Load(db)

	evts, err = models.CloseTickets(ctx, rt, oa, models.NilUserID, []*models.Ticket{modelTicket3}, false, false, logger)
	require.NoError(t, err)
	assert.Equal(t, 1, len(evts))
	assert.Equal(t, models.TicketEventTypeClosed, evts[modelTicket3].EventType())

	testsuite.AssertQuery(t, db, `SELECT count(*) FROM tickets_ticketevent WHERE ticket_id = $1 AND event_type = 'C' AND created_by_id IS NULL`, ticket3.ID).Returns(1)
}

func TestReopenTickets(t *testing.T) {
	ctx, rt, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetData)
	defer httpx.SetRequestor(httpx.DefaultRequestor)

	httpx.SetRequestor(httpx.NewMockRequestor(map[string][]httpx.MockResponse{
		"https://api.mailgun.net/v3/tickets.rapidpro.io/messages": {
			httpx.NewMockResponse(200, nil, `{
				"id": "<20200426161758.1.590432020254B2BF@tickets.rapidpro.io>",
				"message": "Queued. Thank you."
			}`),
		},
	}))

	testdata.InsertContactGroup(db, testdata.Org1, "94c816d7-cc87-42db-a577-ce072ceaab80", "Two Tickets", "tickets = 2")

	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshTicketers|models.RefreshGroups)
	require.NoError(t, err)

	ticket1 := testdata.InsertClosedTicket(db, testdata.Org1, testdata.Cathy, testdata.Mailgun, testdata.DefaultTopic, "Where my shoes", "123", nil)
	modelTicket1 := ticket1.Load(db)

	ticket2 := testdata.InsertOpenTicket(db, testdata.Org1, testdata.Cathy, testdata.Zendesk, testdata.DefaultTopic, "Where my pants", "234", nil)
	modelTicket2 := ticket2.Load(db)

	logger := &models.HTTPLogger{}
	evts, err := models.ReopenTickets(ctx, rt, oa, testdata.Admin.ID, []*models.Ticket{modelTicket1, modelTicket2}, true, logger)
	require.NoError(t, err)
	assert.Equal(t, 1, len(evts))
	assert.Equal(t, models.TicketEventTypeReopened, evts[modelTicket1].EventType())

	// check ticket #1 is now closed
	testsuite.AssertQuery(t, db, `SELECT count(*) FROM tickets_ticket WHERE id = $1 AND status = 'O' AND closed_on IS NULL`, ticket1.ID).Returns(1)

	// and there's reopened event for it
	testsuite.AssertQuery(t, db, `SELECT count(*) FROM tickets_ticketevent WHERE org_id = $1 AND ticket_id = $2 AND event_type = 'R'`, testdata.Org1.ID, ticket1.ID).Returns(1)

	// and the logger has an http log it can insert for that ticketer
	require.NoError(t, logger.Insert(ctx, db))

	testsuite.AssertQuery(t, db, `SELECT count(*) FROM request_logs_httplog WHERE ticketer_id = $1`, testdata.Mailgun.ID).Returns(1)

	// but no events for ticket #2 which waas already open
	testsuite.AssertQuery(t, db, `SELECT count(*) FROM tickets_ticketevent WHERE ticket_id = $1 AND event_type = 'R'`, ticket2.ID).Returns(0)

	// check Cathy is now in the two tickets group
	_, cathy := testdata.Cathy.Load(db, oa)
	assert.Equal(t, 2, len(cathy.Groups().All()))
	assert.Equal(t, "Doctors", cathy.Groups().All()[0].Name())
	assert.Equal(t, "Two Tickets", cathy.Groups().All()[1].Name())
}
