package models_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/gocommon/dbutil/assertdb"
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

	oa := testdata.Org1.Load(rt)

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
		testdata.Org1.ID,
		testdata.Alexandria.ID,
		testdata.Zendesk.ID,
		"EX6677",
		testdata.SupportTopic.ID,
		"Where are my pants?",
		testdata.Admin.ID,
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

	err := models.InsertTickets(ctx, db, oa, []*models.Ticket{ticket1, ticket2, ticket3})
	assert.NoError(t, err)

	// check all tickets were created
	assertdb.Query(t, db, `SELECT count(*) FROM tickets_ticket WHERE status = 'O' AND closed_on IS NULL`).Returns(3)

	// check counts were added
	assertTicketDailyCount(t, db, models.TicketDailyCountOpening, fmt.Sprintf("o:%d", testdata.Org1.ID), 3)
	assertTicketDailyCount(t, db, models.TicketDailyCountOpening, fmt.Sprintf("o:%d", testdata.Org2.ID), 0)
	assertTicketDailyCount(t, db, models.TicketDailyCountAssignment, fmt.Sprintf("o:%d:u:%d", testdata.Org1.ID, testdata.Admin.ID), 2)
	assertTicketDailyCount(t, db, models.TicketDailyCountAssignment, fmt.Sprintf("o:%d:u:%d", testdata.Org1.ID, testdata.Editor.ID), 0)

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

	ticket := testdata.InsertOpenTicket(db, testdata.Org1, testdata.Cathy, testdata.Mailgun, testdata.DefaultTopic, "Where my shoes", "123", time.Now(), nil)
	modelTicket := ticket.Load(db)

	// empty configs are null
	assertdb.Query(t, db, `SELECT count(*) FROM tickets_ticket WHERE config IS NULL AND id = $1`, ticket.ID).Returns(1)

	models.UpdateTicketConfig(ctx, db, modelTicket, map[string]string{"foo": "2352", "bar": "abc"})

	assertdb.Query(t, db, `SELECT count(*) FROM tickets_ticket WHERE config='{"foo": "2352", "bar": "abc"}'::jsonb AND id = $1`, ticket.ID).Returns(1)

	// updates are additive
	models.UpdateTicketConfig(ctx, db, modelTicket, map[string]string{"foo": "6547", "zed": "xyz"})

	assertdb.Query(t, db, `SELECT count(*) FROM tickets_ticket WHERE config='{"foo": "6547", "bar": "abc", "zed": "xyz"}'::jsonb AND id = $1`, ticket.ID).Returns(1)
}

func TestUpdateTicketLastActivity(t *testing.T) {
	ctx, _, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetData)

	now := time.Date(2021, 6, 22, 15, 59, 30, 123456000, time.UTC)

	defer dates.SetNowSource(dates.DefaultNowSource)
	dates.SetNowSource(dates.NewFixedNowSource(now))

	ticket := testdata.InsertOpenTicket(db, testdata.Org1, testdata.Cathy, testdata.Mailgun, testdata.DefaultTopic, "Where my shoes", "123", time.Now(), nil)
	modelTicket := ticket.Load(db)

	models.UpdateTicketLastActivity(ctx, db, []*models.Ticket{modelTicket})

	assert.Equal(t, now, modelTicket.LastActivityOn())

	assertdb.Query(t, db, `SELECT last_activity_on FROM tickets_ticket WHERE id = $1`, ticket.ID).Returns(modelTicket.LastActivityOn())

}

func TestTicketsAssign(t *testing.T) {
	ctx, rt, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetData)

	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshTicketers)
	require.NoError(t, err)

	ticket1 := testdata.InsertClosedTicket(db, testdata.Org1, testdata.Cathy, testdata.Mailgun, testdata.DefaultTopic, "Where my shoes", "123", nil)
	modelTicket1 := ticket1.Load(db)

	ticket2 := testdata.InsertOpenTicket(db, testdata.Org1, testdata.Cathy, testdata.Zendesk, testdata.DefaultTopic, "Where my pants", "234", time.Now(), nil)
	modelTicket2 := ticket2.Load(db)

	// create ticket already assigned to a user
	ticket3 := testdata.InsertOpenTicket(db, testdata.Org1, testdata.Cathy, testdata.Zendesk, testdata.DefaultTopic, "Where my glasses", "", time.Now(), testdata.Admin)
	modelTicket3 := ticket3.Load(db)

	testdata.InsertOpenTicket(db, testdata.Org1, testdata.Cathy, testdata.Mailgun, testdata.DefaultTopic, "", "", time.Now(), nil)

	evts, err := models.TicketsAssign(ctx, db, oa, testdata.Admin.ID, []*models.Ticket{modelTicket1, modelTicket2, modelTicket3}, testdata.Agent.ID, "please handle these")
	require.NoError(t, err)
	assert.Equal(t, 3, len(evts))
	assert.Equal(t, models.TicketEventTypeAssigned, evts[modelTicket1].EventType())
	assert.Equal(t, models.TicketEventTypeAssigned, evts[modelTicket2].EventType())
	assert.Equal(t, models.TicketEventTypeAssigned, evts[modelTicket3].EventType())

	// check tickets are now assigned
	assertdb.Query(t, db, `SELECT assignee_id FROM tickets_ticket WHERE id = $1`, ticket1.ID).Columns(map[string]interface{}{"assignee_id": int64(testdata.Agent.ID)})
	assertdb.Query(t, db, `SELECT assignee_id FROM tickets_ticket WHERE id = $1`, ticket2.ID).Columns(map[string]interface{}{"assignee_id": int64(testdata.Agent.ID)})
	assertdb.Query(t, db, `SELECT assignee_id FROM tickets_ticket WHERE id = $1`, ticket3.ID).Columns(map[string]interface{}{"assignee_id": int64(testdata.Agent.ID)})

	// and there are new assigned events with notifications
	assertdb.Query(t, db, `SELECT count(*) FROM tickets_ticketevent WHERE event_type = 'A' AND note = 'please handle these'`).Returns(3)
	assertdb.Query(t, db, `SELECT count(*) FROM notifications_notification WHERE user_id = $1 AND notification_type = 'tickets:activity'`, testdata.Agent.ID).Returns(1)

	// and daily counts (we only count first assignments of a ticket)
	assertTicketDailyCount(t, db, models.TicketDailyCountAssignment, fmt.Sprintf("o:%d:u:%d", testdata.Org1.ID, testdata.Agent.ID), 2)
	assertTicketDailyCount(t, db, models.TicketDailyCountAssignment, fmt.Sprintf("o:%d:u:%d", testdata.Org1.ID, testdata.Admin.ID), 0)
}

func TestTicketsAddNote(t *testing.T) {
	ctx, rt, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetData)

	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshTicketers)
	require.NoError(t, err)

	ticket1 := testdata.InsertClosedTicket(db, testdata.Org1, testdata.Cathy, testdata.Mailgun, testdata.DefaultTopic, "Where my shoes", "123", nil)
	modelTicket1 := ticket1.Load(db)

	ticket2 := testdata.InsertOpenTicket(db, testdata.Org1, testdata.Cathy, testdata.Zendesk, testdata.DefaultTopic, "Where my pants", "234", time.Now(), testdata.Agent)
	modelTicket2 := ticket2.Load(db)

	testdata.InsertOpenTicket(db, testdata.Org1, testdata.Cathy, testdata.Mailgun, testdata.DefaultTopic, "", "", time.Now(), nil)

	evts, err := models.TicketsAddNote(ctx, db, oa, testdata.Admin.ID, []*models.Ticket{modelTicket1, modelTicket2}, "spam")
	require.NoError(t, err)
	assert.Equal(t, 2, len(evts))
	assert.Equal(t, models.TicketEventTypeNoteAdded, evts[modelTicket1].EventType())
	assert.Equal(t, models.TicketEventTypeNoteAdded, evts[modelTicket2].EventType())

	// check there are new note events
	assertdb.Query(t, db, `SELECT count(*) FROM tickets_ticketevent WHERE event_type = 'N' AND note = 'spam'`).Returns(2)

	assertdb.Query(t, db, `SELECT count(*) FROM notifications_notification WHERE user_id = $1 AND notification_type = 'tickets:activity'`, testdata.Agent.ID).Returns(1)
}

func TestTicketsChangeTopic(t *testing.T) {
	ctx, rt, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetData)

	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshTicketers)
	require.NoError(t, err)

	ticket1 := testdata.InsertClosedTicket(db, testdata.Org1, testdata.Cathy, testdata.Mailgun, testdata.SalesTopic, "Where my shoes", "123", nil)
	modelTicket1 := ticket1.Load(db)

	ticket2 := testdata.InsertOpenTicket(db, testdata.Org1, testdata.Cathy, testdata.Zendesk, testdata.SupportTopic, "Where my pants", "234", time.Now(), nil)
	modelTicket2 := ticket2.Load(db)

	ticket3 := testdata.InsertOpenTicket(db, testdata.Org1, testdata.Cathy, testdata.Zendesk, testdata.DefaultTopic, "Where my pants", "345", time.Now(), nil)
	modelTicket3 := ticket3.Load(db)

	testdata.InsertOpenTicket(db, testdata.Org1, testdata.Cathy, testdata.Mailgun, testdata.DefaultTopic, "", "", time.Now(), nil)

	evts, err := models.TicketsChangeTopic(ctx, db, oa, testdata.Admin.ID, []*models.Ticket{modelTicket1, modelTicket2, modelTicket3}, testdata.SupportTopic.ID)
	require.NoError(t, err)
	assert.Equal(t, 2, len(evts)) // ticket 2 not included as already has that topic
	assert.Equal(t, models.TicketEventTypeTopicChanged, evts[modelTicket1].EventType())
	assert.Equal(t, models.TicketEventTypeTopicChanged, evts[modelTicket3].EventType())

	// check tickets are updated and we have events
	assertdb.Query(t, db, `SELECT count(*) FROM tickets_ticket WHERE topic_id = $1`, testdata.SupportTopic.ID).Returns(3)
	assertdb.Query(t, db, `SELECT count(*) FROM tickets_ticketevent WHERE event_type = 'T' AND topic_id = $1`, testdata.SupportTopic.ID).Returns(2)
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

	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshTicketers|models.RefreshGroups)
	require.NoError(t, err)

	ticket1 := testdata.InsertOpenTicket(db, testdata.Org1, testdata.Cathy, testdata.Mailgun, testdata.DefaultTopic, "Where my shoes", "123", time.Now(), nil)
	modelTicket1 := ticket1.Load(db)

	ticket2 := testdata.InsertClosedTicket(db, testdata.Org1, testdata.Cathy, testdata.Zendesk, testdata.DefaultTopic, "Where my pants", "234", nil)
	modelTicket2 := ticket2.Load(db)

	_, cathy := testdata.Cathy.Load(db, oa)

	err = models.CalculateDynamicGroups(ctx, db, oa, []*flows.Contact{cathy})
	require.NoError(t, err)

	assert.Equal(t, "Doctors", cathy.Groups().All()[0].Name())
	assert.Equal(t, "Open Tickets", cathy.Groups().All()[1].Name())

	logger := &models.HTTPLogger{}
	evts, err := models.CloseTickets(ctx, rt, oa, testdata.Admin.ID, []*models.Ticket{modelTicket1, modelTicket2}, true, false, logger)
	require.NoError(t, err)
	assert.Equal(t, 1, len(evts))
	assert.Equal(t, models.TicketEventTypeClosed, evts[modelTicket1].EventType())

	// check ticket #1 is now closed
	assertdb.Query(t, db, `SELECT count(*) FROM tickets_ticket WHERE id = $1 AND status = 'C' AND closed_on IS NOT NULL`, ticket1.ID).Returns(1)

	// and there's closed event for it
	assertdb.Query(t, db, `SELECT count(*) FROM tickets_ticketevent WHERE org_id = $1 AND ticket_id = $2 AND event_type = 'C'`,
		[]interface{}{testdata.Org1.ID, ticket1.ID}, 1)

	// and the logger has an http log it can insert for that ticketer
	require.NoError(t, logger.Insert(ctx, db))

	assertdb.Query(t, db, `SELECT count(*) FROM request_logs_httplog WHERE ticketer_id = $1`, testdata.Mailgun.ID).Returns(1)

	// reload Cathy and check they're no longer in the tickets group
	_, cathy = testdata.Cathy.Load(db, oa)
	assert.Equal(t, 1, len(cathy.Groups().All()))
	assert.Equal(t, "Doctors", cathy.Groups().All()[0].Name())

	// but no events for ticket #2 which was already closed
	assertdb.Query(t, db, `SELECT count(*) FROM tickets_ticketevent WHERE ticket_id = $1 AND event_type = 'C'`, ticket2.ID).Returns(0)

	// can close tickets without a user
	ticket3 := testdata.InsertOpenTicket(db, testdata.Org1, testdata.Cathy, testdata.Mailgun, testdata.DefaultTopic, "Where my shoes", "123", time.Now(), nil)
	modelTicket3 := ticket3.Load(db)

	evts, err = models.CloseTickets(ctx, rt, oa, models.NilUserID, []*models.Ticket{modelTicket3}, false, false, logger)
	require.NoError(t, err)
	assert.Equal(t, 1, len(evts))
	assert.Equal(t, models.TicketEventTypeClosed, evts[modelTicket3].EventType())

	assertdb.Query(t, db, `SELECT count(*) FROM tickets_ticketevent WHERE ticket_id = $1 AND event_type = 'C' AND created_by_id IS NULL`, ticket3.ID).Returns(1)
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

	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshTicketers|models.RefreshGroups)
	require.NoError(t, err)

	ticket1 := testdata.InsertClosedTicket(db, testdata.Org1, testdata.Cathy, testdata.Mailgun, testdata.DefaultTopic, "Where my shoes", "123", nil)
	modelTicket1 := ticket1.Load(db)

	ticket2 := testdata.InsertOpenTicket(db, testdata.Org1, testdata.Cathy, testdata.Zendesk, testdata.DefaultTopic, "Where my pants", "234", time.Now(), nil)
	modelTicket2 := ticket2.Load(db)

	logger := &models.HTTPLogger{}
	evts, err := models.ReopenTickets(ctx, rt, oa, testdata.Admin.ID, []*models.Ticket{modelTicket1, modelTicket2}, true, logger)
	require.NoError(t, err)
	assert.Equal(t, 1, len(evts))
	assert.Equal(t, models.TicketEventTypeReopened, evts[modelTicket1].EventType())

	// check ticket #1 is now closed
	assertdb.Query(t, db, `SELECT count(*) FROM tickets_ticket WHERE id = $1 AND status = 'O' AND closed_on IS NULL`, ticket1.ID).Returns(1)

	// and there's reopened event for it
	assertdb.Query(t, db, `SELECT count(*) FROM tickets_ticketevent WHERE org_id = $1 AND ticket_id = $2 AND event_type = 'R'`, testdata.Org1.ID, ticket1.ID).Returns(1)

	// and the logger has an http log it can insert for that ticketer
	require.NoError(t, logger.Insert(ctx, db))

	assertdb.Query(t, db, `SELECT count(*) FROM request_logs_httplog WHERE ticketer_id = $1`, testdata.Mailgun.ID).Returns(1)

	// but no events for ticket #2 which waas already open
	assertdb.Query(t, db, `SELECT count(*) FROM tickets_ticketevent WHERE ticket_id = $1 AND event_type = 'R'`, ticket2.ID).Returns(0)

	// check Cathy is now in the open tickets group
	_, cathy := testdata.Cathy.Load(db, oa)
	assert.Equal(t, 2, len(cathy.Groups().All()))
	assert.Equal(t, "Doctors", cathy.Groups().All()[0].Name())
	assert.Equal(t, "Open Tickets", cathy.Groups().All()[1].Name())

	// reopening doesn't change opening daily counts
	assertTicketDailyCount(t, db, models.TicketDailyCountOpening, fmt.Sprintf("o:%d", testdata.Org1.ID), 0)
}

func TestTicketRecordReply(t *testing.T) {
	ctx, _, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetData)

	openedOn := time.Date(2022, 5, 18, 14, 21, 0, 0, time.UTC)
	repliedOn := time.Date(2022, 5, 18, 15, 0, 0, 0, time.UTC)

	ticket := testdata.InsertOpenTicket(db, testdata.Org1, testdata.Cathy, testdata.Mailgun, testdata.DefaultTopic, "Where my shoes", "123", openedOn, nil)

	timing, err := models.TicketRecordReplied(ctx, db, ticket.ID, repliedOn)
	assert.NoError(t, err)
	assert.Equal(t, 2340*time.Second, timing)

	modelTicket := ticket.Load(db)
	assert.Equal(t, repliedOn, *modelTicket.RepliedOn())
	assert.Equal(t, repliedOn, modelTicket.LastActivityOn())

	assertdb.Query(t, db, `SELECT replied_on FROM tickets_ticket WHERE id = $1`, ticket.ID).Returns(repliedOn)
	assertdb.Query(t, db, `SELECT last_activity_on FROM tickets_ticket WHERE id = $1`, ticket.ID).Returns(repliedOn)

	repliedAgainOn := time.Date(2022, 5, 18, 15, 5, 0, 0, time.UTC)

	// if we call it again, it won't change replied_on again but it will update last_activity_on
	timing, err = models.TicketRecordReplied(ctx, db, ticket.ID, repliedAgainOn)
	assert.NoError(t, err)
	assert.Equal(t, time.Duration(-1), timing)

	modelTicket = ticket.Load(db)
	assert.Equal(t, repliedOn, *modelTicket.RepliedOn())
	assert.Equal(t, repliedAgainOn, modelTicket.LastActivityOn())

	assertdb.Query(t, db, `SELECT replied_on FROM tickets_ticket WHERE id = $1`, ticket.ID).Returns(repliedOn)
	assertdb.Query(t, db, `SELECT last_activity_on FROM tickets_ticket WHERE id = $1`, ticket.ID).Returns(repliedAgainOn)
}

func assertTicketDailyCount(t *testing.T, db *sqlx.DB, countType models.TicketDailyCountType, scope string, expected int) {
	assertdb.Query(t, db, `SELECT COALESCE(SUM(count), 0) FROM tickets_ticketdailycount WHERE count_type = $1 AND scope = $2`, countType, scope).Returns(expected)
}
