package models_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTickets(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetData)

	oa := testdata.Org1.Load(rt)

	ticket1 := models.NewTicket(
		"2ef57efc-d85f-4291-b330-e4afe68af5fe",
		testdata.Org1.ID,
		testdata.Admin.ID,
		models.NilFlowID,
		testdata.Cathy.ID,
		testdata.DefaultTopic.ID,
		"Where are my cookies?",
		testdata.Admin.ID,
	)
	ticket2 := models.NewTicket(
		"64f81be1-00ff-48ef-9e51-97d6f924c1a4",
		testdata.Org1.ID,
		testdata.Admin.ID,
		models.NilFlowID,
		testdata.Bob.ID,
		testdata.SalesTopic.ID,
		"Where are my trousers?",
		models.NilUserID,
	)
	ticket3 := models.NewTicket(
		"28ef8ddc-b221-42f3-aeae-ee406fc9d716",
		testdata.Org1.ID,
		models.NilUserID,
		testdata.Favorites.ID,
		testdata.Alexandria.ID,
		testdata.SupportTopic.ID,
		"Where are my pants?",
		testdata.Admin.ID,
	)

	assert.Equal(t, flows.TicketUUID("2ef57efc-d85f-4291-b330-e4afe68af5fe"), ticket1.UUID())
	assert.Equal(t, testdata.Org1.ID, ticket1.OrgID())
	assert.Equal(t, testdata.Cathy.ID, ticket1.ContactID())
	assert.Equal(t, testdata.DefaultTopic.ID, ticket1.TopicID())
	assert.Equal(t, testdata.Admin.ID, ticket1.AssigneeID())

	err := models.InsertTickets(ctx, rt.DB, oa, []*models.Ticket{ticket1, ticket2, ticket3})
	assert.NoError(t, err)

	// check all tickets were created
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM tickets_ticket WHERE status = 'O' AND closed_on IS NULL`).Returns(3)

	// check counts were added
	assertTicketDailyCount(t, rt, models.TicketDailyCountOpening, fmt.Sprintf("o:%d", testdata.Org1.ID), 3)
	assertTicketDailyCount(t, rt, models.TicketDailyCountOpening, fmt.Sprintf("o:%d", testdata.Org2.ID), 0)
	assertTicketDailyCount(t, rt, models.TicketDailyCountAssignment, fmt.Sprintf("o:%d:u:%d", testdata.Org1.ID, testdata.Admin.ID), 2)
	assertTicketDailyCount(t, rt, models.TicketDailyCountAssignment, fmt.Sprintf("o:%d:u:%d", testdata.Org1.ID, testdata.Editor.ID), 0)

	// can lookup a ticket by UUID
	tk1, err := models.LookupTicketByUUID(ctx, rt.DB, "2ef57efc-d85f-4291-b330-e4afe68af5fe")
	assert.NoError(t, err)
	assert.Equal(t, "Where are my cookies?", tk1.Body())

	// can lookup open tickets by contact
	org1, _ := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
	cathy, err := models.LoadContact(ctx, rt.DB, org1, testdata.Cathy.ID)
	require.NoError(t, err)

	tk, err := models.LoadOpenTicketForContact(ctx, rt.DB, cathy)
	assert.NoError(t, err)
	assert.NotNil(t, tk)
	assert.Equal(t, "Where are my cookies?", tk.Body())
}

func TestUpdateTicketLastActivity(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetData)

	now := time.Date(2021, 6, 22, 15, 59, 30, 123456000, time.UTC)

	defer dates.SetNowSource(dates.DefaultNowSource)
	dates.SetNowSource(dates.NewFixedNowSource(now))

	ticket := testdata.InsertOpenTicket(rt, testdata.Org1, testdata.Cathy, testdata.DefaultTopic, "Where my shoes", time.Now(), nil)
	modelTicket := ticket.Load(rt)

	models.UpdateTicketLastActivity(ctx, rt.DB, []*models.Ticket{modelTicket})

	assert.Equal(t, now, modelTicket.LastActivityOn())

	assertdb.Query(t, rt.DB, `SELECT last_activity_on FROM tickets_ticket WHERE id = $1`, ticket.ID).Returns(modelTicket.LastActivityOn())

}

func TestTicketsAssign(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetData)

	oa, err := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
	require.NoError(t, err)

	ticket1 := testdata.InsertClosedTicket(rt, testdata.Org1, testdata.Cathy, testdata.DefaultTopic, "Where my shoes", nil)
	modelTicket1 := ticket1.Load(rt)

	ticket2 := testdata.InsertOpenTicket(rt, testdata.Org1, testdata.Cathy, testdata.DefaultTopic, "Where my pants", time.Now(), nil)
	modelTicket2 := ticket2.Load(rt)

	// create ticket already assigned to a user
	ticket3 := testdata.InsertOpenTicket(rt, testdata.Org1, testdata.Cathy, testdata.DefaultTopic, "Where my glasses", time.Now(), testdata.Admin)
	modelTicket3 := ticket3.Load(rt)

	testdata.InsertOpenTicket(rt, testdata.Org1, testdata.Cathy, testdata.DefaultTopic, "", time.Now(), nil)

	evts, err := models.TicketsAssign(ctx, rt.DB, oa, testdata.Admin.ID, []*models.Ticket{modelTicket1, modelTicket2, modelTicket3}, testdata.Agent.ID)
	require.NoError(t, err)
	assert.Equal(t, 3, len(evts))
	assert.Equal(t, models.TicketEventTypeAssigned, evts[modelTicket1].EventType())
	assert.Equal(t, models.TicketEventTypeAssigned, evts[modelTicket2].EventType())
	assert.Equal(t, models.TicketEventTypeAssigned, evts[modelTicket3].EventType())

	// check tickets are now assigned
	assertdb.Query(t, rt.DB, `SELECT assignee_id FROM tickets_ticket WHERE id = $1`, ticket1.ID).Columns(map[string]any{"assignee_id": int64(testdata.Agent.ID)})
	assertdb.Query(t, rt.DB, `SELECT assignee_id FROM tickets_ticket WHERE id = $1`, ticket2.ID).Columns(map[string]any{"assignee_id": int64(testdata.Agent.ID)})
	assertdb.Query(t, rt.DB, `SELECT assignee_id FROM tickets_ticket WHERE id = $1`, ticket3.ID).Columns(map[string]any{"assignee_id": int64(testdata.Agent.ID)})

	// and there are new assigned events with notifications
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM tickets_ticketevent WHERE event_type = 'A'`).Returns(3)
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM notifications_notification WHERE user_id = $1 AND notification_type = 'tickets:activity'`, testdata.Agent.ID).Returns(1)

	// and daily counts (we only count first assignments of a ticket)
	assertTicketDailyCount(t, rt, models.TicketDailyCountAssignment, fmt.Sprintf("o:%d:u:%d", testdata.Org1.ID, testdata.Agent.ID), 2)
	assertTicketDailyCount(t, rt, models.TicketDailyCountAssignment, fmt.Sprintf("o:%d:u:%d", testdata.Org1.ID, testdata.Admin.ID), 0)
}

func TestTicketsAddNote(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetData)

	oa, err := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
	require.NoError(t, err)

	ticket1 := testdata.InsertClosedTicket(rt, testdata.Org1, testdata.Cathy, testdata.DefaultTopic, "Where my shoes", nil)
	modelTicket1 := ticket1.Load(rt)

	ticket2 := testdata.InsertOpenTicket(rt, testdata.Org1, testdata.Cathy, testdata.DefaultTopic, "Where my pants", time.Now(), testdata.Agent)
	modelTicket2 := ticket2.Load(rt)

	testdata.InsertOpenTicket(rt, testdata.Org1, testdata.Cathy, testdata.DefaultTopic, "", time.Now(), nil)

	evts, err := models.TicketsAddNote(ctx, rt.DB, oa, testdata.Admin.ID, []*models.Ticket{modelTicket1, modelTicket2}, "spam")
	require.NoError(t, err)
	assert.Equal(t, 2, len(evts))
	assert.Equal(t, models.TicketEventTypeNoteAdded, evts[modelTicket1].EventType())
	assert.Equal(t, models.TicketEventTypeNoteAdded, evts[modelTicket2].EventType())

	// check there are new note events
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM tickets_ticketevent WHERE event_type = 'N' AND note = 'spam'`).Returns(2)

	assertdb.Query(t, rt.DB, `SELECT count(*) FROM notifications_notification WHERE user_id = $1 AND notification_type = 'tickets:activity'`, testdata.Agent.ID).Returns(1)
}

func TestTicketsChangeTopic(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetData)

	oa, err := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
	require.NoError(t, err)

	ticket1 := testdata.InsertClosedTicket(rt, testdata.Org1, testdata.Cathy, testdata.SalesTopic, "Where my shoes", nil)
	modelTicket1 := ticket1.Load(rt)

	ticket2 := testdata.InsertOpenTicket(rt, testdata.Org1, testdata.Cathy, testdata.SupportTopic, "Where my pants", time.Now(), nil)
	modelTicket2 := ticket2.Load(rt)

	ticket3 := testdata.InsertOpenTicket(rt, testdata.Org1, testdata.Cathy, testdata.DefaultTopic, "Where my pants", time.Now(), nil)
	modelTicket3 := ticket3.Load(rt)

	testdata.InsertOpenTicket(rt, testdata.Org1, testdata.Cathy, testdata.DefaultTopic, "", time.Now(), nil)

	evts, err := models.TicketsChangeTopic(ctx, rt.DB, oa, testdata.Admin.ID, []*models.Ticket{modelTicket1, modelTicket2, modelTicket3}, testdata.SupportTopic.ID)
	require.NoError(t, err)
	assert.Equal(t, 2, len(evts)) // ticket 2 not included as already has that topic
	assert.Equal(t, models.TicketEventTypeTopicChanged, evts[modelTicket1].EventType())
	assert.Equal(t, models.TicketEventTypeTopicChanged, evts[modelTicket3].EventType())

	// check tickets are updated and we have events
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM tickets_ticket WHERE topic_id = $1`, testdata.SupportTopic.ID).Returns(3)
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM tickets_ticketevent WHERE event_type = 'T' AND topic_id = $1`, testdata.SupportTopic.ID).Returns(2)
}

func TestCloseTickets(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetData)

	oa, err := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
	require.NoError(t, err)

	ticket1 := testdata.InsertOpenTicket(rt, testdata.Org1, testdata.Cathy, testdata.DefaultTopic, "Where my shoes", time.Now(), nil)
	modelTicket1 := ticket1.Load(rt)

	ticket2 := testdata.InsertClosedTicket(rt, testdata.Org1, testdata.Cathy, testdata.DefaultTopic, "Where my pants", nil)
	modelTicket2 := ticket2.Load(rt)

	_, cathy, _ := testdata.Cathy.Load(rt, oa)

	err = models.CalculateDynamicGroups(ctx, rt.DB, oa, []*flows.Contact{cathy})
	require.NoError(t, err)

	assert.Equal(t, "Doctors", cathy.Groups().All()[0].Name())
	assert.Equal(t, "Open Tickets", cathy.Groups().All()[1].Name())

	evts, err := models.CloseTickets(ctx, rt, oa, testdata.Admin.ID, []*models.Ticket{modelTicket1, modelTicket2})
	require.NoError(t, err)
	assert.Equal(t, 1, len(evts))
	assert.Equal(t, models.TicketEventTypeClosed, evts[modelTicket1].EventType())

	// check ticket #1 is now closed
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM tickets_ticket WHERE id = $1 AND status = 'C' AND closed_on IS NOT NULL`, ticket1.ID).Returns(1)

	// and there's closed event for it
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM tickets_ticketevent WHERE org_id = $1 AND ticket_id = $2 AND event_type = 'C'`,
		[]any{testdata.Org1.ID, ticket1.ID}, 1)

	// reload Cathy and check they're no longer in the tickets group
	_, cathy, _ = testdata.Cathy.Load(rt, oa)
	assert.Equal(t, 1, len(cathy.Groups().All()))
	assert.Equal(t, "Doctors", cathy.Groups().All()[0].Name())

	// but no events for ticket #2 which was already closed
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM tickets_ticketevent WHERE ticket_id = $1 AND event_type = 'C'`, ticket2.ID).Returns(0)

	// can close tickets without a user
	ticket3 := testdata.InsertOpenTicket(rt, testdata.Org1, testdata.Cathy, testdata.DefaultTopic, "Where my shoes", time.Now(), nil)
	modelTicket3 := ticket3.Load(rt)

	evts, err = models.CloseTickets(ctx, rt, oa, models.NilUserID, []*models.Ticket{modelTicket3})
	require.NoError(t, err)
	assert.Equal(t, 1, len(evts))
	assert.Equal(t, models.TicketEventTypeClosed, evts[modelTicket3].EventType())

	assertdb.Query(t, rt.DB, `SELECT count(*) FROM tickets_ticketevent WHERE ticket_id = $1 AND event_type = 'C' AND created_by_id IS NULL`, ticket3.ID).Returns(1)
}

func TestReopenTickets(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetData)

	oa, err := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
	require.NoError(t, err)

	ticket1 := testdata.InsertClosedTicket(rt, testdata.Org1, testdata.Cathy, testdata.DefaultTopic, "Where my shoes", nil)
	modelTicket1 := ticket1.Load(rt)

	ticket2 := testdata.InsertOpenTicket(rt, testdata.Org1, testdata.Cathy, testdata.DefaultTopic, "Where my pants", time.Now(), nil)
	modelTicket2 := ticket2.Load(rt)

	evts, err := models.ReopenTickets(ctx, rt, oa, testdata.Admin.ID, []*models.Ticket{modelTicket1, modelTicket2})
	require.NoError(t, err)
	assert.Equal(t, 1, len(evts))
	assert.Equal(t, models.TicketEventTypeReopened, evts[modelTicket1].EventType())

	// check ticket #1 is now closed
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM tickets_ticket WHERE id = $1 AND status = 'O' AND closed_on IS NULL`, ticket1.ID).Returns(1)

	// and there's reopened event for it
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM tickets_ticketevent WHERE org_id = $1 AND ticket_id = $2 AND event_type = 'R'`, testdata.Org1.ID, ticket1.ID).Returns(1)

	// but no events for ticket #2 which waas already open
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM tickets_ticketevent WHERE ticket_id = $1 AND event_type = 'R'`, ticket2.ID).Returns(0)

	// check Cathy is now in the open tickets group
	_, cathy, _ := testdata.Cathy.Load(rt, oa)
	assert.Equal(t, 2, len(cathy.Groups().All()))
	assert.Equal(t, "Doctors", cathy.Groups().All()[0].Name())
	assert.Equal(t, "Open Tickets", cathy.Groups().All()[1].Name())

	// reopening doesn't change opening daily counts
	assertTicketDailyCount(t, rt, models.TicketDailyCountOpening, fmt.Sprintf("o:%d", testdata.Org1.ID), 0)
}

func TestTicketRecordReply(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetData)

	openedOn := time.Date(2022, 5, 18, 14, 21, 0, 0, time.UTC)
	repliedOn := time.Date(2022, 5, 18, 15, 0, 0, 0, time.UTC)

	ticket := testdata.InsertOpenTicket(rt, testdata.Org1, testdata.Cathy, testdata.DefaultTopic, "Where my shoes", openedOn, nil)

	timing, err := models.TicketRecordReplied(ctx, rt.DB, ticket.ID, repliedOn)
	assert.NoError(t, err)
	assert.Equal(t, 2340*time.Second, timing)

	modelTicket := ticket.Load(rt)
	assert.Equal(t, repliedOn, *modelTicket.RepliedOn())
	assert.Equal(t, repliedOn, modelTicket.LastActivityOn())

	assertdb.Query(t, rt.DB, `SELECT replied_on FROM tickets_ticket WHERE id = $1`, ticket.ID).Returns(repliedOn)
	assertdb.Query(t, rt.DB, `SELECT last_activity_on FROM tickets_ticket WHERE id = $1`, ticket.ID).Returns(repliedOn)

	repliedAgainOn := time.Date(2022, 5, 18, 15, 5, 0, 0, time.UTC)

	// if we call it again, it won't change replied_on again but it will update last_activity_on
	timing, err = models.TicketRecordReplied(ctx, rt.DB, ticket.ID, repliedAgainOn)
	assert.NoError(t, err)
	assert.Equal(t, time.Duration(-1), timing)

	modelTicket = ticket.Load(rt)
	assert.Equal(t, repliedOn, *modelTicket.RepliedOn())
	assert.Equal(t, repliedAgainOn, modelTicket.LastActivityOn())

	assertdb.Query(t, rt.DB, `SELECT replied_on FROM tickets_ticket WHERE id = $1`, ticket.ID).Returns(repliedOn)
	assertdb.Query(t, rt.DB, `SELECT last_activity_on FROM tickets_ticket WHERE id = $1`, ticket.ID).Returns(repliedAgainOn)
}

func assertTicketDailyCount(t *testing.T, rt *runtime.Runtime, countType models.TicketDailyCountType, scope string, expected int) {
	assertdb.Query(t, rt.DB, `SELECT COALESCE(SUM(count), 0) FROM tickets_ticketdailycount WHERE count_type = $1 AND scope = $2`, countType, scope).Returns(expected)
}
