package handler_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/buger/jsonparser"
	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/flows"
	_ "github.com/nyaruka/mailroom/core/handlers"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/queue"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/core/tasks/handler"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMsgEvents(t *testing.T) {
	ctx, rt := testsuite.Runtime()
	rc := rt.RP.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetAll)

	testdata.InsertKeywordTrigger(rt, testdata.Org1, testdata.Favorites, []string{"start"}, models.MatchOnly, nil, nil, nil)
	testdata.InsertKeywordTrigger(rt, testdata.Org1, testdata.IVRFlow, []string{"ivr"}, models.MatchOnly, nil, nil, nil)

	testdata.InsertKeywordTrigger(rt, testdata.Org2, testdata.Org2Favorites, []string{"start"}, models.MatchOnly, nil, nil, nil)
	testdata.InsertCatchallTrigger(rt, testdata.Org2, testdata.Org2SingleMessage, nil, nil, nil)

	// give Cathy and Bob some tickets...
	openTickets := map[*testdata.Contact][]*testdata.Ticket{
		testdata.Cathy: {
			testdata.InsertOpenTicket(rt, testdata.Org1, testdata.Cathy, testdata.DefaultTopic, "Ok", time.Now(), nil),
		},
	}
	closedTickets := map[*testdata.Contact][]*testdata.Ticket{
		testdata.Cathy: {
			testdata.InsertClosedTicket(rt, testdata.Org1, testdata.Cathy, testdata.DefaultTopic, "", nil),
		},
		testdata.Bob: {
			testdata.InsertClosedTicket(rt, testdata.Org1, testdata.Bob, testdata.DefaultTopic, "Ok", nil),
		},
	}

	rt.DB.MustExec(`UPDATE tickets_ticket SET last_activity_on = '2021-01-01T00:00:00Z'`)

	// clear all of Alexandria's URNs
	rt.DB.MustExec(`UPDATE contacts_contacturn SET contact_id = NULL WHERE contact_id = $1`, testdata.Alexandria.ID)

	models.FlushCache()

	// insert a dummy message into the database that will get the updates from handling each message event which pretends to be it
	dbMsg := testdata.InsertIncomingMsg(rt, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "", models.MsgStatusPending)

	tcs := []struct {
		preHook       func()
		org           *testdata.Org
		channel       *testdata.Channel
		contact       *testdata.Contact
		text          string
		expectedReply string
		expectedFlow  *testdata.Flow
	}{
		// 0:
		{
			org:           testdata.Org1,
			channel:       testdata.FacebookChannel,
			contact:       testdata.Cathy,
			text:          "noop",
			expectedReply: "",
		},

		// 1:
		{
			org:           testdata.Org1,
			channel:       testdata.FacebookChannel,
			contact:       testdata.Cathy,
			text:          "start other",
			expectedReply: "",
		},

		// 2:
		{
			org:           testdata.Org1,
			channel:       testdata.FacebookChannel,
			contact:       testdata.Cathy,
			text:          "start",
			expectedReply: "What is your favorite color?",
			expectedFlow:  testdata.Favorites,
		},

		// 3:
		{
			org:           testdata.Org1,
			channel:       testdata.FacebookChannel,
			contact:       testdata.Cathy,
			text:          "purple",
			expectedReply: "I don't know that color. Try again.",
			expectedFlow:  testdata.Favorites,
		},

		// 4:
		{
			org:           testdata.Org1,
			channel:       testdata.FacebookChannel,
			contact:       testdata.Cathy,
			text:          "blue",
			expectedReply: "Good choice, I like Blue too! What is your favorite beer?",
			expectedFlow:  testdata.Favorites,
		},

		// 5:
		{
			org:           testdata.Org1,
			channel:       testdata.FacebookChannel,
			contact:       testdata.Cathy,
			text:          "MUTZIG",
			expectedReply: "Mmmmm... delicious Mutzig. If only they made blue Mutzig! Lastly, what is your name?",
			expectedFlow:  testdata.Favorites,
		},

		// 6:
		{
			org:           testdata.Org1,
			channel:       testdata.FacebookChannel,
			contact:       testdata.Cathy,
			text:          "Cathy",
			expectedReply: "Thanks Cathy, we are all done!",
			expectedFlow:  testdata.Favorites,
		},

		// 7:
		{
			org:           testdata.Org1,
			channel:       testdata.FacebookChannel,
			contact:       testdata.Cathy,
			text:          "noop",
			expectedReply: "",
		},

		// 8:
		{
			org:           testdata.Org2,
			channel:       testdata.Org2Channel,
			contact:       testdata.Org2Contact,
			text:          "other",
			expectedReply: "Hey, how are you?",
			expectedFlow:  testdata.Org2SingleMessage,
		},

		// 9:
		{
			org:           testdata.Org2,
			channel:       testdata.Org2Channel,
			contact:       testdata.Org2Contact,
			text:          "start",
			expectedReply: "What is your favorite color?",
			expectedFlow:  testdata.Org2Favorites,
		},

		// 10:
		{
			org:           testdata.Org2,
			channel:       testdata.Org2Channel,
			contact:       testdata.Org2Contact,
			text:          "green",
			expectedReply: "Good choice, I like Green too! What is your favorite beer?",
			expectedFlow:  testdata.Org2Favorites,
		},

		// 11:
		{
			org:           testdata.Org2,
			channel:       testdata.Org2Channel,
			contact:       testdata.Org2Contact,
			text:          "primus",
			expectedReply: "Mmmmm... delicious Primus. If only they made green Primus! Lastly, what is your name?",
			expectedFlow:  testdata.Org2Favorites,
		},

		// 12:
		{
			org:           testdata.Org2,
			channel:       testdata.Org2Channel,
			contact:       testdata.Org2Contact,
			text:          "george",
			expectedReply: "Thanks george, we are all done!",
			expectedFlow:  testdata.Org2Favorites,
		},

		// 13:
		{
			org:           testdata.Org2,
			channel:       testdata.Org2Channel,
			contact:       testdata.Org2Contact,
			text:          "blargh",
			expectedReply: "Hey, how are you?",
			expectedFlow:  testdata.Org2SingleMessage,
		},

		// 14:
		{
			org:           testdata.Org1,
			channel:       testdata.FacebookChannel,
			contact:       testdata.Bob,
			text:          "ivr",
			expectedReply: "",
			expectedFlow:  testdata.IVRFlow,
		},

		// 15: stopped contact should be unstopped
		{
			preHook: func() {
				rt.DB.MustExec(`UPDATE contacts_contact SET status = 'S' WHERE id = $1`, testdata.George.ID)
			},
			org:           testdata.Org1,
			channel:       testdata.FacebookChannel,
			contact:       testdata.George,
			text:          "start",
			expectedReply: "What is your favorite color?",
			expectedFlow:  testdata.Favorites,
		},

		// 16: no URN on contact but handle event, session gets started but no message created
		{
			org:           testdata.Org1,
			channel:       testdata.TwilioChannel,
			contact:       testdata.Alexandria,
			text:          "start",
			expectedReply: "",
			expectedFlow:  testdata.Favorites,
		},

		// 17: start Fred back in our favorite flow, then make it inactive, will be handled by catch-all
		{
			org:           testdata.Org2,
			channel:       testdata.Org2Channel,
			contact:       testdata.Org2Contact,
			text:          "start",
			expectedReply: "What is your favorite color?",
			expectedFlow:  testdata.Org2Favorites,
		},

		// 18:
		{
			preHook: func() {
				rt.DB.MustExec(`UPDATE flows_flow SET is_active = FALSE WHERE id = $1`, testdata.Org2Favorites.ID)
			},
			org:           testdata.Org2,
			channel:       testdata.Org2Channel,
			contact:       testdata.Org2Contact,
			text:          "red",
			expectedReply: "Hey, how are you?",
			expectedFlow:  testdata.Org2SingleMessage,
		},

		// 19: start Fred back in our favorites flow to test retries
		{
			preHook: func() {
				rt.DB.MustExec(`UPDATE flows_flow SET is_active = TRUE WHERE id = $1`, testdata.Org2Favorites.ID)
			},
			org:           testdata.Org2,
			channel:       testdata.Org2Channel,
			contact:       testdata.Org2Contact,
			text:          "start",
			expectedReply: "What is your favorite color?",
			expectedFlow:  testdata.Org2Favorites,
		},
	}

	makeMsgTask := func(org *testdata.Org, channel *testdata.Channel, contact *testdata.Contact, text string) *queue.Task {
		return &queue.Task{Type: handler.MsgEventType, OrgID: int(org.ID), Task: jsonx.MustMarshal(&handler.MsgEvent{
			ContactID: contact.ID,
			OrgID:     org.ID,
			ChannelID: channel.ID,
			MsgID:     dbMsg.ID,
			MsgUUID:   dbMsg.FlowMsg.UUID(),
			URN:       contact.URN,
			URNID:     contact.URNID,
			Text:      text,
		})}
	}

	last := time.Now()

	for i, tc := range tcs {
		models.FlushCache()

		// reset our dummy db message into an unhandled state
		rt.DB.MustExec(`UPDATE msgs_msg SET status = 'P', flow_id = NULL WHERE id = $1`, dbMsg.ID)

		// run our setup hook if we have one
		if tc.preHook != nil {
			tc.preHook()
		}

		task := makeMsgTask(tc.org, tc.channel, tc.contact, tc.text)

		err := handler.QueueHandleTask(rc, tc.contact.ID, task)
		assert.NoError(t, err, "%d: error adding task", i)

		task, err = queue.PopNextTask(rc, queue.HandlerQueue)
		assert.NoError(t, err, "%d: error popping next task", i)

		err = tasks.Perform(ctx, rt, task)
		assert.NoError(t, err, "%d: error when handling event", i)

		var expectedFlowID any
		if tc.expectedFlow != nil {
			expectedFlowID = int64(tc.expectedFlow.ID)
		}

		// check that message is marked as handled
		assertdb.Query(t, rt.DB, `SELECT status, msg_type, flow_id FROM msgs_msg WHERE id = $1`, dbMsg.ID).
			Columns(map[string]any{"status": "H", "msg_type": "T", "flow_id": expectedFlowID}, "%d: msg state mismatch", i)

		// if we are meant to have a reply, check it
		if tc.expectedReply != "" {
			assertdb.Query(t, rt.DB, `SELECT text, status FROM msgs_msg WHERE contact_id = $1 AND created_on > $2 ORDER BY id DESC LIMIT 1`, tc.contact.ID, last).
				Columns(map[string]any{"text": tc.expectedReply, "status": "Q"}, "%d: response mismatch", i)
		}

		// check last open ticket for this contact was updated
		numOpenTickets := len(openTickets[tc.contact])
		assertdb.Query(t, rt.DB, `SELECT count(*) FROM tickets_ticket WHERE contact_id = $1 AND status = 'O' AND last_activity_on > $2`, tc.contact.ID, last).
			Returns(numOpenTickets, "%d: updated open ticket mismatch", i)

		// check any closed tickets are unchanged
		numClosedTickets := len(closedTickets[tc.contact])
		assertdb.Query(t, rt.DB, `SELECT count(*) FROM tickets_ticket WHERE contact_id = $1 AND status = 'C' AND last_activity_on = '2021-01-01T00:00:00Z'`, tc.contact.ID).
			Returns(numClosedTickets, "%d: unchanged closed ticket mismatch", i)

		last = time.Now()
	}

	// should have one remaining IVR task to handle for Bob
	orgTasks := testsuite.CurrentTasks(t, rt)
	assert.Equal(t, 1, len(orgTasks[testdata.Org1.ID]))

	task, err := queue.PopNextTask(rc, queue.BatchQueue)
	assert.NoError(t, err)
	assert.NotNil(t, task)
	assert.Equal(t, "start_ivr_flow_batch", task.Type)

	// check messages queued to courier
	testsuite.AssertCourierQueues(t, map[string][]int{
		fmt.Sprintf("msgs:%s|10/1", testdata.FacebookChannel.UUID): {1, 1, 1, 1, 1, 1},
		fmt.Sprintf("msgs:%s|10/1", testdata.Org2Channel.UUID):     {1, 1, 1, 1, 1, 1, 1, 1, 1},
	})

	// Fred's sessions should not have a timeout because courier will set them
	assertdb.Query(t, rt.DB, `SELECT count(*) from flows_flowsession where contact_id = $1`, testdata.Org2Contact.ID).Returns(6)
	assertdb.Query(t, rt.DB, `SELECT count(*) from flows_flowsession where contact_id = $1 and timeout_on IS NULL`, testdata.Org2Contact.ID).Returns(6)

	// force an error by marking our run for fred as complete (our session is still active so this will blow up)
	rt.DB.MustExec(`UPDATE flows_flowrun SET status = 'C', exited_on = NOW() WHERE contact_id = $1`, testdata.Org2Contact.ID)
	task = makeMsgTask(testdata.Org2, testdata.Org2Channel, testdata.Org2Contact, "red")
	handler.QueueHandleTask(rc, testdata.Org2Contact.ID, task)

	// should get requeued three times automatically
	for i := 0; i < 3; i++ {
		task, _ = queue.PopNextTask(rc, queue.HandlerQueue)
		assert.NotNil(t, task)
		err := tasks.Perform(ctx, rt, task)
		assert.NoError(t, err)
	}

	// on third error, no new task
	task, err = queue.PopNextTask(rc, queue.HandlerQueue)
	assert.NoError(t, err)
	assert.Nil(t, task)

	// mark Fred's flow as inactive
	rt.DB.MustExec(`UPDATE flows_flow SET is_active = FALSE where id = $1`, testdata.Org2Favorites.ID)
	models.FlushCache()

	// try to resume now
	task = makeMsgTask(testdata.Org2, testdata.Org2Channel, testdata.Org2Contact, "red")
	handler.QueueHandleTask(rc, testdata.Org2Contact.ID, task)
	task, _ = queue.PopNextTask(rc, queue.HandlerQueue)
	assert.NotNil(t, task)
	err = tasks.Perform(ctx, rt, task)
	assert.NoError(t, err)

	// should get our catch all trigger
	assertdb.Query(t, rt.DB, `SELECT text FROM msgs_msg WHERE contact_id = $1 AND direction = 'O' ORDER BY id DESC LIMIT 1`, testdata.Org2Contact.ID).Returns("Hey, how are you?")
	previous := time.Now()

	// and should have failed previous session
	assertdb.Query(t, rt.DB, `SELECT count(*) from flows_flowsession where contact_id = $1 and status = 'F'`, testdata.Org2Contact.ID).Returns(2)

	// trigger should also not start a new session
	task = makeMsgTask(testdata.Org2, testdata.Org2Channel, testdata.Org2Contact, "start")
	handler.QueueHandleTask(rc, testdata.Org2Contact.ID, task)
	task, _ = queue.PopNextTask(rc, queue.HandlerQueue)
	err = tasks.Perform(ctx, rt, task)
	assert.NoError(t, err)

	assertdb.Query(t, rt.DB, `SELECT count(*) FROM msgs_msg WHERE contact_id = $1 AND direction = 'O' AND created_on > $2`, testdata.Org2Contact.ID, previous).Returns(0)
}

func TestChannelEvents(t *testing.T) {
	ctx, rt := testsuite.Runtime()
	rc := rt.RP.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetAll)

	// add some channel event triggers
	testdata.InsertNewConversationTrigger(rt, testdata.Org1, testdata.Favorites, testdata.FacebookChannel)
	testdata.InsertReferralTrigger(rt, testdata.Org1, testdata.PickANumber, "", testdata.VonageChannel)
	testdata.InsertOptInTrigger(rt, testdata.Org1, testdata.Favorites, testdata.VonageChannel)
	testdata.InsertOptOutTrigger(rt, testdata.Org1, testdata.PickANumber, testdata.VonageChannel)

	polls := testdata.InsertOptIn(rt, testdata.Org1, "Polls")

	// add a URN for cathy so we can test twitter URNs
	testdata.InsertContactURN(rt, testdata.Org1, testdata.Bob, urns.URN("twitterid:123456"), 10, nil)

	tcs := []struct {
		EventType           models.ChannelEventType
		ContactID           models.ContactID
		URNID               models.URNID
		ChannelID           models.ChannelID
		OptInID             models.OptInID
		Extra               map[string]any
		expectedTriggerType string
		expectedResponse    string
		updatesLastSeen     bool
	}{
		{
			models.EventTypeNewConversation,
			testdata.Cathy.ID,
			testdata.Cathy.URNID,
			testdata.FacebookChannel.ID,
			models.NilOptInID,
			nil,
			"channel",
			"What is your favorite color?",
			true,
		},
		{
			models.EventTypeNewConversation,
			testdata.Cathy.ID,
			testdata.Cathy.URNID,
			testdata.VonageChannel.ID,
			models.NilOptInID,
			nil,
			"",
			"",
			true,
		},
		{
			models.EventTypeWelcomeMessage,
			testdata.Cathy.ID,
			testdata.Cathy.URNID,
			testdata.VonageChannel.ID,
			models.NilOptInID,
			nil,
			"",
			"",
			false,
		},
		{
			models.EventTypeReferral,
			testdata.Cathy.ID,
			testdata.Cathy.URNID,
			testdata.FacebookChannel.ID,
			models.NilOptInID,
			nil,
			"",
			"",
			true,
		},
		{
			models.EventTypeReferral,
			testdata.Cathy.ID, testdata.Cathy.URNID,
			testdata.VonageChannel.ID,
			models.NilOptInID,
			nil,
			"channel",
			"Pick a number between 1-10.",
			true,
		},
		{
			models.EventTypeOptIn,
			testdata.Cathy.ID,
			testdata.Cathy.URNID,
			testdata.VonageChannel.ID,
			polls.ID,
			map[string]any{"title": "Polls", "payload": fmt.Sprint(polls.ID)},
			"optin",
			"What is your favorite color?",
			true,
		},
		{
			models.EventTypeOptOut,
			testdata.Cathy.ID,
			testdata.Cathy.URNID,
			testdata.VonageChannel.ID,
			polls.ID,
			map[string]any{"title": "Polls", "payload": fmt.Sprint(polls.ID)},
			"optin",
			"Pick a number between 1-10.",
			true,
		},
	}

	models.FlushCache()

	for i, tc := range tcs {
		start := time.Now()
		time.Sleep(time.Millisecond * 5)

		event := models.NewChannelEvent(tc.EventType, testdata.Org1.ID, tc.ChannelID, tc.ContactID, tc.URNID, tc.OptInID, tc.Extra, false)
		err := event.Insert(ctx, rt.DB)
		require.NoError(t, err)

		task := &queue.Task{
			Type:  string(tc.EventType),
			OrgID: int(testdata.Org1.ID),
			Task:  jsonx.MustMarshal(event),
		}

		err = handler.QueueHandleTask(rc, tc.ContactID, task)
		assert.NoError(t, err, "%d: error adding task", i)

		task, err = queue.PopNextTask(rc, queue.HandlerQueue)
		assert.NoError(t, err, "%d: error popping next task", i)

		err = tasks.Perform(ctx, rt, task)
		assert.NoError(t, err, "%d: error when handling event", i)

		// if we are meant to trigger a new session...
		if tc.expectedTriggerType != "" {
			assertdb.Query(t, rt.DB, `SELECT count(*) FROM flows_flowsession WHERE contact_id = $1 AND created_on > $2`, tc.ContactID, start).Returns(1)

			var output []byte
			err = rt.DB.Get(&output, `SELECT output FROM flows_flowsession WHERE contact_id = $1 AND created_on > $2`, tc.ContactID, start)
			require.NoError(t, err)

			trigType, err := jsonparser.GetString(output, "trigger", "type")
			require.NoError(t, err)
			assert.Equal(t, tc.expectedTriggerType, trigType)

			assertdb.Query(t, rt.DB, `SELECT text FROM msgs_msg WHERE contact_id = $1 AND contact_urn_id = $2 AND created_on > $3 ORDER BY id DESC LIMIT 1`, tc.ContactID, tc.URNID, start).
				Returns(tc.expectedResponse, "%d: response mismatch", i)
		}

		if tc.updatesLastSeen {
			var lastSeen time.Time
			err = rt.DB.Get(&lastSeen, `SELECT last_seen_on FROM contacts_contact WHERE id = $1`, tc.ContactID)
			assert.NoError(t, err)
			assert.True(t, lastSeen.Equal(start) || lastSeen.After(start), "%d: expected last seen to be updated", i)
		}
	}
}

func TestTicketEvents(t *testing.T) {
	ctx, rt := testsuite.Runtime()
	rc := rt.RP.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetAll)

	// add a ticket closed trigger
	testdata.InsertTicketClosedTrigger(rt, testdata.Org1, testdata.Favorites)

	ticket := testdata.InsertClosedTicket(rt, testdata.Org1, testdata.Cathy, testdata.DefaultTopic, "Where are my shoes?", nil)
	modelTicket := ticket.Load(rt)

	event := models.NewTicketClosedEvent(modelTicket, testdata.Admin.ID)

	err := handler.QueueTicketEvent(rc, testdata.Cathy.ID, event)
	require.NoError(t, err)

	task, err := queue.PopNextTask(rc, queue.HandlerQueue)
	require.NoError(t, err)

	err = tasks.Perform(ctx, rt, task)
	require.NoError(t, err)

	assertdb.Query(t, rt.DB, `SELECT count(*) FROM msgs_msg WHERE contact_id = $1 AND direction = 'O' AND text = 'What is your favorite color?'`, testdata.Cathy.ID).Returns(1)
}

func TestStopEvent(t *testing.T) {
	ctx, rt := testsuite.Runtime()
	rc := rt.RP.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetAll)

	// schedule an event for cathy and george
	testdata.InsertEventFire(rt, testdata.Cathy, testdata.RemindersEvent1, time.Now())
	testdata.InsertEventFire(rt, testdata.George, testdata.RemindersEvent1, time.Now())

	// and george to doctors group, cathy is already part of it
	rt.DB.MustExec(`INSERT INTO contacts_contactgroup_contacts(contactgroup_id, contact_id) VALUES($1, $2);`, testdata.DoctorsGroup.ID, testdata.George.ID)

	event := &handler.StopEvent{OrgID: testdata.Org1.ID, ContactID: testdata.Cathy.ID}
	eventJSON, err := json.Marshal(event)
	require.NoError(t, err)
	task := &queue.Task{
		Type:  string(models.EventTypeStopContact),
		OrgID: int(testdata.Org1.ID),
		Task:  eventJSON,
	}

	err = handler.QueueHandleTask(rc, testdata.Cathy.ID, task)
	assert.NoError(t, err, "error adding task")

	task, err = queue.PopNextTask(rc, queue.HandlerQueue)
	assert.NoError(t, err, "error popping next task")

	err = tasks.Perform(ctx, rt, task)
	assert.NoError(t, err, "error when handling event")

	// check that only george is in our group
	assertdb.Query(t, rt.DB, `SELECT count(*) from contacts_contactgroup_contacts WHERE contactgroup_id = $1 AND contact_id = $2`, testdata.DoctorsGroup.ID, testdata.Cathy.ID).Returns(0)
	assertdb.Query(t, rt.DB, `SELECT count(*) from contacts_contactgroup_contacts WHERE contactgroup_id = $1 AND contact_id = $2`, testdata.DoctorsGroup.ID, testdata.George.ID).Returns(1)

	// that cathy is stopped
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM contacts_contact WHERE id = $1 AND status = 'S'`, testdata.Cathy.ID).Returns(1)

	// and has no upcoming events
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM campaigns_eventfire WHERE contact_id = $1`, testdata.Cathy.ID).Returns(0)
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM campaigns_eventfire WHERE contact_id = $1`, testdata.George.ID).Returns(1)
}

func TestTimedEvents(t *testing.T) {
	ctx, rt := testsuite.Runtime()
	rc := rt.RP.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetAll)

	// create some keyword triggers
	testdata.InsertKeywordTrigger(rt, testdata.Org1, testdata.Favorites, []string{"start"}, models.MatchOnly, nil, nil, nil)
	testdata.InsertKeywordTrigger(rt, testdata.Org1, testdata.PickANumber, []string{"pick"}, models.MatchOnly, nil, nil, nil)

	tcs := []struct {
		EventType string
		Contact   *testdata.Contact
		Message   string
		Response  string
		Channel   *testdata.Channel
		Org       *testdata.Org
	}{
		// 0: start the flow
		{handler.MsgEventType, testdata.Cathy, "start", "What is your favorite color?", testdata.FacebookChannel, testdata.Org1},

		// 1: this expiration does nothing because the times don't match
		{handler.ExpirationEventType, testdata.Cathy, "bad", "", testdata.FacebookChannel, testdata.Org1},

		// 2: this checks that the flow wasn't expired
		{handler.MsgEventType, testdata.Cathy, "red", "Good choice, I like Red too! What is your favorite beer?", testdata.FacebookChannel, testdata.Org1},

		// 3: this expiration will actually take
		{handler.ExpirationEventType, testdata.Cathy, "good", "", testdata.FacebookChannel, testdata.Org1},

		// 4: we won't get a response as we will be out of the flow
		{handler.MsgEventType, testdata.Cathy, "mutzig", "", testdata.FacebookChannel, testdata.Org1},

		// 5: start the parent expiration flow
		{handler.MsgEventType, testdata.Cathy, "parent", "Child", testdata.FacebookChannel, testdata.Org1},

		// 6: respond, should bring us out
		{handler.MsgEventType, testdata.Cathy, "hi", "Completed", testdata.FacebookChannel, testdata.Org1},

		// 7: expiring our child should be a no op
		{handler.ExpirationEventType, testdata.Cathy, "child", "", testdata.FacebookChannel, testdata.Org1},

		// 8: respond one last time, should be done
		{handler.MsgEventType, testdata.Cathy, "done", "Ended", testdata.FacebookChannel, testdata.Org1},

		// 9: start our favorite flow again
		{handler.MsgEventType, testdata.Cathy, "start", "What is your favorite color?", testdata.FacebookChannel, testdata.Org1},

		// 10: timeout on the color question
		{handler.TimeoutEventType, testdata.Cathy, "", "Sorry you can't participate right now, I'll try again later.", testdata.FacebookChannel, testdata.Org1},

		// 11: start the pick a number flow
		{handler.MsgEventType, testdata.Cathy, "pick", "Pick a number between 1-10.", testdata.FacebookChannel, testdata.Org1},

		// 12: try to resume with timeout even tho flow doesn't have one set
		{handler.TimeoutEventType, testdata.Cathy, "", "", testdata.FacebookChannel, testdata.Org1},
	}

	last := time.Now()
	var sessionID models.SessionID
	var runID models.FlowRunID

	for i, tc := range tcs {
		time.Sleep(50 * time.Millisecond)

		var task *queue.Task

		if tc.EventType == handler.MsgEventType {
			task = &queue.Task{
				Type:  tc.EventType,
				OrgID: int(tc.Org.ID),
				Task: jsonx.MustMarshal(&handler.MsgEvent{
					ContactID: tc.Contact.ID,
					OrgID:     tc.Org.ID,
					ChannelID: tc.Channel.ID,
					MsgID:     models.MsgID(1),
					MsgUUID:   flows.MsgUUID(uuids.New()),
					URN:       tc.Contact.URN,
					URNID:     tc.Contact.URNID,
					Text:      tc.Message,
				}),
			}
		} else if tc.EventType == handler.ExpirationEventType {
			var expiration time.Time

			if tc.Message == "bad" {
				expiration = time.Now()
			} else if tc.Message == "child" {
				rt.DB.Get(&expiration, `SELECT wait_expires_on FROM flows_flowsession WHERE id = $1 AND status != 'W'`, sessionID)
				rt.DB.Get(&runID, `SELECT id FROM flows_flowrun WHERE session_id = $1 AND status NOT IN ('A', 'W')`, sessionID)
			} else {
				expiration = time.Now().Add(time.Hour * 24)
			}

			task = handler.NewExpirationTask(tc.Org.ID, tc.Contact.ID, sessionID, expiration)

		} else if tc.EventType == handler.TimeoutEventType {
			timeoutOn := time.Now().Round(time.Millisecond) // so that there's no difference between this and what we read from the db

			// usually courier will set timeout_on after sending the last message
			rt.DB.MustExec(`UPDATE flows_flowsession SET timeout_on = $2 WHERE id = $1`, sessionID, timeoutOn)

			task = handler.NewTimeoutTask(tc.Org.ID, tc.Contact.ID, sessionID, timeoutOn)
		}

		err := handler.QueueHandleTask(rc, tc.Contact.ID, task)
		assert.NoError(t, err, "%d: error adding task", i)

		task, err = queue.PopNextTask(rc, queue.HandlerQueue)
		assert.NoError(t, err, "%d: error popping next task", i)

		err = tasks.Perform(ctx, rt, task)
		assert.NoError(t, err, "%d: error when handling event", i)

		if tc.Response != "" {
			assertdb.Query(t, rt.DB, `SELECT text FROM msgs_msg WHERE contact_id = $1 AND created_on > $2 ORDER BY id DESC LIMIT 1`, tc.Contact.ID, last).
				Returns(tc.Response, "%d: response: mismatch", i)
		}

		err = rt.DB.Get(&sessionID, `SELECT id FROM flows_flowsession WHERE contact_id = $1 ORDER BY created_on DESC LIMIT 1`, tc.Contact.ID)
		assert.NoError(t, err)

		err = rt.DB.Get(&runID, `SELECT id FROM flows_flowrun WHERE contact_id = $1 ORDER BY created_on DESC LIMIT 1`, tc.Contact.ID)
		assert.NoError(t, err)

		last = time.Now()
	}

	// should only have a single waiting session/run with no timeout
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM flows_flowsession WHERE status = 'W' AND contact_id = $1`, testdata.Cathy.ID).Returns(1)
	assertdb.Query(t, rt.DB, `SELECT timeout_on FROM flows_flowsession WHERE status = 'W' AND contact_id = $1`, testdata.Cathy.ID).Returns(nil)
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM flows_flowrun WHERE status = 'W' AND contact_id = $1`, testdata.Cathy.ID).Returns(1)

	// test the case of a run and session no longer being the most recent but somehow still active, expiration should still work
	r, err := rt.DB.QueryContext(ctx, `SELECT id, session_id from flows_flowrun WHERE contact_id = $1 and status = 'I' order by created_on asc limit 1`, testdata.Cathy.ID)
	assert.NoError(t, err)
	defer r.Close()
	r.Next()
	r.Scan(&runID, &sessionID)

	expiration := time.Now()

	// set both to be active (this requires us to disable the status change triggers)
	rt.DB.MustExec(`ALTER TABLE flows_flowrun DISABLE TRIGGER temba_flowrun_status_change`)
	rt.DB.MustExec(`ALTER TABLE flows_flowsession DISABLE TRIGGER temba_flowsession_status_change`)
	rt.DB.MustExec(`UPDATE flows_flowrun SET status = 'W' WHERE id = $1`, runID)
	rt.DB.MustExec(`UPDATE flows_flowsession SET status = 'W', wait_started_on = NOW(), wait_expires_on = $2 WHERE id = $1`, sessionID, expiration)
	rt.DB.MustExec(`ALTER TABLE flows_flowrun ENABLE TRIGGER temba_flowrun_status_change`)
	rt.DB.MustExec(`ALTER TABLE flows_flowsession ENABLE TRIGGER temba_flowsession_status_change`)

	// try to expire the run
	task := handler.NewExpirationTask(testdata.Org1.ID, testdata.Cathy.ID, sessionID, expiration)

	err = handler.QueueHandleTask(rc, testdata.Cathy.ID, task)
	assert.NoError(t, err)

	task, err = queue.PopNextTask(rc, queue.HandlerQueue)
	assert.NoError(t, err)

	err = tasks.Perform(ctx, rt, task)
	assert.NoError(t, err)
}
