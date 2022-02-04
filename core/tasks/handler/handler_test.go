package handler_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/flows"
	_ "github.com/nyaruka/mailroom/core/handlers"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/queue"
	"github.com/nyaruka/mailroom/core/tasks/handler"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMsgEvents(t *testing.T) {
	ctx, rt, db, rp := testsuite.Get()
	rc := rp.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetAll)

	testdata.InsertKeywordTrigger(db, testdata.Org1, testdata.Favorites, "start", models.MatchOnly, nil, nil)
	testdata.InsertKeywordTrigger(db, testdata.Org1, testdata.IVRFlow, "ivr", models.MatchOnly, nil, nil)

	testdata.InsertKeywordTrigger(db, testdata.Org2, testdata.Org2Favorites, "start", models.MatchOnly, nil, nil)
	testdata.InsertCatchallTrigger(db, testdata.Org2, testdata.Org2SingleMessage, nil, nil)

	// give Cathy and Bob some tickets...
	openTickets := map[*testdata.Contact][]*testdata.Ticket{
		testdata.Cathy: {
			testdata.InsertOpenTicket(db, testdata.Org1, testdata.Cathy, testdata.Mailgun, testdata.DefaultTopic, "Ok", "", nil),
			testdata.InsertOpenTicket(db, testdata.Org1, testdata.Cathy, testdata.Zendesk, testdata.DefaultTopic, "Ok", "", nil),
		},
	}
	closedTickets := map[*testdata.Contact][]*testdata.Ticket{
		testdata.Cathy: {
			testdata.InsertClosedTicket(db, testdata.Org1, testdata.Cathy, testdata.Mailgun, testdata.DefaultTopic, "", "", nil),
		},
		testdata.Bob: {
			testdata.InsertClosedTicket(db, testdata.Org1, testdata.Bob, testdata.Mailgun, testdata.DefaultTopic, "Ok", "", nil),
		},
	}

	db.MustExec(`UPDATE tickets_ticket SET last_activity_on = '2021-01-01T00:00:00Z'`)

	// clear all of Alexandria's URNs
	db.MustExec(`UPDATE contacts_contacturn SET contact_id = NULL WHERE contact_id = $1`, testdata.Alexandria.ID)

	models.FlushCache()

	// insert a dummy message into the database that will get the updates from handling each message event which pretends to be it
	dbMsg := testdata.InsertIncomingMsg(db, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "", models.MsgStatusPending)

	tcs := []struct {
		preHook       func()
		org           *testdata.Org
		channel       *testdata.Channel
		contact       *testdata.Contact
		text          string
		expectedReply string
		expectedType  models.MsgType
		expectedFlow  *testdata.Flow
	}{
		{
			org:           testdata.Org1,
			channel:       testdata.TwitterChannel,
			contact:       testdata.Cathy,
			text:          "noop",
			expectedReply: "",
			expectedType:  models.MsgTypeInbox,
		},
		{
			org:           testdata.Org1,
			channel:       testdata.TwitterChannel,
			contact:       testdata.Cathy,
			text:          "start other",
			expectedReply: "",
			expectedType:  models.MsgTypeInbox,
		},
		{
			org:           testdata.Org1,
			channel:       testdata.TwitterChannel,
			contact:       testdata.Cathy,
			text:          "start",
			expectedReply: "What is your favorite color?",
			expectedType:  models.MsgTypeFlow,
			expectedFlow:  testdata.Favorites,
		},
		{
			org:           testdata.Org1,
			channel:       testdata.TwitterChannel,
			contact:       testdata.Cathy,
			text:          "purple",
			expectedReply: "I don't know that color. Try again.",
			expectedType:  models.MsgTypeFlow,
			expectedFlow:  testdata.Favorites,
		},
		{
			org:           testdata.Org1,
			channel:       testdata.TwitterChannel,
			contact:       testdata.Cathy,
			text:          "blue",
			expectedReply: "Good choice, I like Blue too! What is your favorite beer?",
			expectedType:  models.MsgTypeFlow,
			expectedFlow:  testdata.Favorites,
		},
		{
			org:           testdata.Org1,
			channel:       testdata.TwitterChannel,
			contact:       testdata.Cathy,
			text:          "MUTZIG",
			expectedReply: "Mmmmm... delicious Mutzig. If only they made blue Mutzig! Lastly, what is your name?",
			expectedType:  models.MsgTypeFlow,
			expectedFlow:  testdata.Favorites,
		},
		{
			org:           testdata.Org1,
			channel:       testdata.TwitterChannel,
			contact:       testdata.Cathy,
			text:          "Cathy",
			expectedReply: "Thanks Cathy, we are all done!",
			expectedType:  models.MsgTypeFlow,
			expectedFlow:  testdata.Favorites,
		},
		{
			org:           testdata.Org1,
			channel:       testdata.TwitterChannel,
			contact:       testdata.Cathy,
			text:          "noop",
			expectedReply: "",
			expectedType:  models.MsgTypeInbox,
		},

		{
			org:           testdata.Org2,
			channel:       testdata.Org2Channel,
			contact:       testdata.Org2Contact,
			text:          "other",
			expectedReply: "Hey, how are you?",
			expectedType:  models.MsgTypeFlow,
			expectedFlow:  testdata.Org2SingleMessage,
		},
		{
			org:           testdata.Org2,
			channel:       testdata.Org2Channel,
			contact:       testdata.Org2Contact,
			text:          "start",
			expectedReply: "What is your favorite color?",
			expectedType:  models.MsgTypeFlow,
			expectedFlow:  testdata.Org2Favorites,
		},
		{
			org:           testdata.Org2,
			channel:       testdata.Org2Channel,
			contact:       testdata.Org2Contact,
			text:          "green",
			expectedReply: "Good choice, I like Green too! What is your favorite beer?",
			expectedType:  models.MsgTypeFlow,
			expectedFlow:  testdata.Org2Favorites,
		},
		{
			org:           testdata.Org2,
			channel:       testdata.Org2Channel,
			contact:       testdata.Org2Contact,
			text:          "primus",
			expectedReply: "Mmmmm... delicious Primus. If only they made green Primus! Lastly, what is your name?",
			expectedType:  models.MsgTypeFlow,
			expectedFlow:  testdata.Org2Favorites,
		},
		{
			org:           testdata.Org2,
			channel:       testdata.Org2Channel,
			contact:       testdata.Org2Contact,
			text:          "george",
			expectedReply: "Thanks george, we are all done!",
			expectedType:  models.MsgTypeFlow,
			expectedFlow:  testdata.Org2Favorites,
		},
		{
			org:           testdata.Org2,
			channel:       testdata.Org2Channel,
			contact:       testdata.Org2Contact,
			text:          "blargh",
			expectedReply: "Hey, how are you?",
			expectedType:  models.MsgTypeFlow,
			expectedFlow:  testdata.Org2SingleMessage,
		},

		{
			org:           testdata.Org1,
			channel:       testdata.TwitterChannel,
			contact:       testdata.Bob,
			text:          "ivr",
			expectedReply: "",
			expectedType:  models.MsgTypeFlow,
			expectedFlow:  testdata.IVRFlow,
		},

		// no URN on contact but handle event, session gets started but no message created
		{
			org:           testdata.Org1,
			channel:       testdata.TwilioChannel,
			contact:       testdata.Alexandria,
			text:          "start",
			expectedReply: "",
			expectedType:  models.MsgTypeFlow,
			expectedFlow:  testdata.Favorites,
		},

		// start Fred back in our favorite flow, then make it inactive, will be handled by catch-all
		{
			org:           testdata.Org2,
			channel:       testdata.Org2Channel,
			contact:       testdata.Org2Contact,
			text:          "start",
			expectedReply: "What is your favorite color?",
			expectedType:  models.MsgTypeFlow,
			expectedFlow:  testdata.Org2Favorites,
		},
		{
			preHook: func() {
				db.MustExec(`UPDATE flows_flow SET is_active = FALSE WHERE id = $1`, testdata.Org2Favorites.ID)
			},
			org:           testdata.Org2,
			channel:       testdata.Org2Channel,
			contact:       testdata.Org2Contact,
			text:          "red",
			expectedReply: "Hey, how are you?",
			expectedType:  models.MsgTypeFlow,
			expectedFlow:  testdata.Org2SingleMessage,
		},

		// start Fred back in our favorites flow to test retries
		{
			preHook: func() {
				db.MustExec(`UPDATE flows_flow SET is_active = TRUE WHERE id = $1`, testdata.Org2Favorites.ID)
			},
			org:           testdata.Org2,
			channel:       testdata.Org2Channel,
			contact:       testdata.Org2Contact,
			text:          "start",
			expectedReply: "What is your favorite color?",
			expectedType:  models.MsgTypeFlow,
			expectedFlow:  testdata.Org2Favorites,
		},
	}

	makeMsgTask := func(org *testdata.Org, channel *testdata.Channel, contact *testdata.Contact, text string) *queue.Task {
		return &queue.Task{Type: handler.MsgEventType, OrgID: int(org.ID), Task: jsonx.MustMarshal(&handler.MsgEvent{
			ContactID: contact.ID,
			OrgID:     org.ID,
			ChannelID: channel.ID,
			MsgID:     dbMsg.ID(),
			MsgUUID:   dbMsg.UUID(),
			URN:       contact.URN,
			URNID:     contact.URNID,
			Text:      text,
		})}
	}

	last := time.Now()

	for i, tc := range tcs {
		models.FlushCache()

		// reset our dummy db message into an unhandled state
		db.MustExec(`UPDATE msgs_msg SET status = 'P', msg_type = NULL WHERE id = $1`, dbMsg.ID())

		// run our setup hook if we have one
		if tc.preHook != nil {
			tc.preHook()
		}

		task := makeMsgTask(tc.org, tc.channel, tc.contact, tc.text)

		err := handler.QueueHandleTask(rc, tc.contact.ID, task)
		assert.NoError(t, err, "%d: error adding task", i)

		task, err = queue.PopNextTask(rc, queue.HandlerQueue)
		assert.NoError(t, err, "%d: error popping next task", i)

		err = handler.HandleEvent(ctx, rt, task)
		assert.NoError(t, err, "%d: error when handling event", i)

		var expectedFlowID interface{}
		if tc.expectedFlow != nil {
			expectedFlowID = int64(tc.expectedFlow.ID)
		}

		// check that message is marked as handled with expected type
		assertdb.Query(t, db, `SELECT status, msg_type, flow_id FROM msgs_msg WHERE id = $1`, dbMsg.ID()).
			Columns(map[string]interface{}{"status": "H", "msg_type": string(tc.expectedType), "flow_id": expectedFlowID}, "%d: msg state mismatch", i)

		// if we are meant to have a reply, check it
		if tc.expectedReply != "" {
			assertdb.Query(t, db, `SELECT text FROM msgs_msg WHERE contact_id = $1 AND created_on > $2 ORDER BY id DESC LIMIT 1`, tc.contact.ID, last).
				Returns(tc.expectedReply, "%d: response mismatch", i)
		}

		// check any open tickets for this contact where updated
		numOpenTickets := len(openTickets[tc.contact])
		assertdb.Query(t, db, `SELECT count(*) FROM tickets_ticket WHERE contact_id = $1 AND status = 'O' AND last_activity_on > $2`, tc.contact.ID, last).
			Returns(numOpenTickets, "%d: updated open ticket mismatch", i)

		// check any closed tickets are unchanged
		numClosedTickets := len(closedTickets[tc.contact])
		assertdb.Query(t, db, `SELECT count(*) FROM tickets_ticket WHERE contact_id = $1 AND status = 'C' AND last_activity_on = '2021-01-01T00:00:00Z'`, tc.contact.ID).
			Returns(numClosedTickets, "%d: unchanged closed ticket mismatch", i)

		last = time.Now()
	}

	// should have one remaining IVR task to handle for Bob
	orgTasks := testsuite.CurrentOrgTasks(t, rp)
	assert.Equal(t, 1, len(orgTasks[testdata.Org1.ID]))

	task, err := queue.PopNextTask(rc, queue.BatchQueue)
	assert.NoError(t, err)
	assert.NotNil(t, task)
	assert.Equal(t, queue.StartIVRFlowBatch, task.Type)

	// check messages queued to courier
	testsuite.AssertCourierQueues(t, map[string][]int{
		fmt.Sprintf("msgs:%s|10/1", testdata.TwitterChannel.UUID): {1, 1, 1, 1, 1},
		fmt.Sprintf("msgs:%s|10/1", testdata.Org2Channel.UUID):    {1, 1, 1, 1, 1, 1, 1, 1, 1},
	})

	// Fred's sessions should not have a timeout because courier will set them
	assertdb.Query(t, db, `SELECT count(*) from flows_flowsession where contact_id = $1`, testdata.Org2Contact.ID).Returns(6)
	assertdb.Query(t, db, `SELECT count(*) from flows_flowsession where contact_id = $1 and timeout_on IS NULL`, testdata.Org2Contact.ID).Returns(6)

	// force an error by marking our run for fred as complete (our session is still active so this will blow up)
	db.MustExec(`UPDATE flows_flowrun SET status = 'C' WHERE contact_id = $1`, testdata.Org2Contact.ID)
	task = makeMsgTask(testdata.Org2, testdata.Org2Channel, testdata.Org2Contact, "red")
	handler.QueueHandleTask(rc, testdata.Org2Contact.ID, task)

	// should get requeued three times automatically
	for i := 0; i < 3; i++ {
		task, _ = queue.PopNextTask(rc, queue.HandlerQueue)
		assert.NotNil(t, task)
		err := handler.HandleEvent(ctx, rt, task)
		assert.NoError(t, err)
	}

	// on third error, no new task
	task, err = queue.PopNextTask(rc, queue.HandlerQueue)
	assert.NoError(t, err)
	assert.Nil(t, task)

	// mark Fred's flow as inactive
	db.MustExec(`UPDATE flows_flow SET is_active = FALSE where id = $1`, testdata.Org2Favorites.ID)
	models.FlushCache()

	// try to resume now
	task = makeMsgTask(testdata.Org2, testdata.Org2Channel, testdata.Org2Contact, "red")
	handler.QueueHandleTask(rc, testdata.Org2Contact.ID, task)
	task, _ = queue.PopNextTask(rc, queue.HandlerQueue)
	assert.NotNil(t, task)
	err = handler.HandleEvent(ctx, rt, task)
	assert.NoError(t, err)

	// should get our catch all trigger
	assertdb.Query(t, db, `SELECT text FROM msgs_msg WHERE contact_id = $1 AND direction = 'O' ORDER BY id DESC LIMIT 1`, testdata.Org2Contact.ID).Returns("Hey, how are you?")
	previous := time.Now()

	// and should have failed previous session
	assertdb.Query(t, db, `SELECT count(*) from flows_flowsession where contact_id = $1 and status = 'F'`, testdata.Org2Contact.ID).Returns(2)

	// trigger should also not start a new session
	task = makeMsgTask(testdata.Org2, testdata.Org2Channel, testdata.Org2Contact, "start")
	handler.QueueHandleTask(rc, testdata.Org2Contact.ID, task)
	task, _ = queue.PopNextTask(rc, queue.HandlerQueue)
	err = handler.HandleEvent(ctx, rt, task)
	assert.NoError(t, err)

	assertdb.Query(t, db, `SELECT count(*) FROM msgs_msg WHERE contact_id = $1 AND direction = 'O' AND created_on > $2`, testdata.Org2Contact.ID, previous).Returns(0)
}

func TestChannelEvents(t *testing.T) {
	ctx, rt, db, rp := testsuite.Get()
	rc := rp.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetAll)

	// add some channel event triggers
	testdata.InsertNewConversationTrigger(db, testdata.Org1, testdata.Favorites, testdata.TwitterChannel)
	testdata.InsertReferralTrigger(db, testdata.Org1, testdata.PickANumber, "", testdata.VonageChannel)

	// add a URN for cathy so we can test twitter URNs
	testdata.InsertContactURN(db, testdata.Org1, testdata.Bob, urns.URN("twitterid:123456"), 10)

	tcs := []struct {
		EventType      models.ChannelEventType
		ContactID      models.ContactID
		URNID          models.URNID
		OrgID          models.OrgID
		ChannelID      models.ChannelID
		Extra          map[string]interface{}
		Response       string
		UpdateLastSeen bool
	}{
		{handler.NewConversationEventType, testdata.Cathy.ID, testdata.Cathy.URNID, testdata.Org1.ID, testdata.TwitterChannel.ID, nil, "What is your favorite color?", true},
		{handler.NewConversationEventType, testdata.Cathy.ID, testdata.Cathy.URNID, testdata.Org1.ID, testdata.VonageChannel.ID, nil, "", true},
		{handler.WelcomeMessageEventType, testdata.Cathy.ID, testdata.Cathy.URNID, testdata.Org1.ID, testdata.VonageChannel.ID, nil, "", false},
		{handler.ReferralEventType, testdata.Cathy.ID, testdata.Cathy.URNID, testdata.Org1.ID, testdata.TwitterChannel.ID, nil, "", true},
		{handler.ReferralEventType, testdata.Cathy.ID, testdata.Cathy.URNID, testdata.Org1.ID, testdata.VonageChannel.ID, nil, "Pick a number between 1-10.", true},
	}

	models.FlushCache()

	for i, tc := range tcs {
		start := time.Now()
		time.Sleep(time.Millisecond * 5)

		event := models.NewChannelEvent(tc.EventType, tc.OrgID, tc.ChannelID, tc.ContactID, tc.URNID, tc.Extra, false)
		eventJSON, err := json.Marshal(event)
		assert.NoError(t, err)

		task := &queue.Task{
			Type:  string(tc.EventType),
			OrgID: int(tc.OrgID),
			Task:  eventJSON,
		}

		err = handler.QueueHandleTask(rc, tc.ContactID, task)
		assert.NoError(t, err, "%d: error adding task", i)

		task, err = queue.PopNextTask(rc, queue.HandlerQueue)
		assert.NoError(t, err, "%d: error popping next task", i)

		err = handler.HandleEvent(ctx, rt, task)
		assert.NoError(t, err, "%d: error when handling event", i)

		// if we are meant to have a response
		if tc.Response != "" {
			assertdb.Query(t, db, `SELECT text FROM msgs_msg WHERE contact_id = $1 AND contact_urn_id = $2 AND created_on > $3 ORDER BY id DESC LIMIT 1`, tc.ContactID, tc.URNID, start).
				Returns(tc.Response, "%d: response mismatch", i)
		}

		if tc.UpdateLastSeen {
			var lastSeen time.Time
			err = db.Get(&lastSeen, `SELECT last_seen_on FROM contacts_contact WHERE id = $1`, tc.ContactID)
			assert.NoError(t, err)
			assert.True(t, lastSeen.Equal(start) || lastSeen.After(start), "%d: expected last seen to be updated", i)
		}
	}
}

func TestTicketEvents(t *testing.T) {
	ctx, rt, db, _ := testsuite.Get()
	rc := rt.RP.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetAll)

	// add a ticket closed trigger
	testdata.InsertTicketClosedTrigger(rt.DB, testdata.Org1, testdata.Favorites)

	ticket := testdata.InsertClosedTicket(rt.DB, testdata.Org1, testdata.Cathy, testdata.Mailgun, testdata.DefaultTopic, "Where are my shoes?", "", nil)
	modelTicket := ticket.Load(db)

	event := models.NewTicketClosedEvent(modelTicket, testdata.Admin.ID)

	err := handler.QueueTicketEvent(rc, testdata.Cathy.ID, event)
	require.NoError(t, err)

	task, err := queue.PopNextTask(rc, queue.HandlerQueue)
	require.NoError(t, err)

	err = handler.HandleEvent(ctx, rt, task)
	require.NoError(t, err)

	assertdb.Query(t, rt.DB, `SELECT count(*) FROM msgs_msg WHERE contact_id = $1 AND direction = 'O' AND text = 'What is your favorite color?'`, testdata.Cathy.ID).Returns(1)
}

func TestStopEvent(t *testing.T) {
	ctx, rt, db, rp := testsuite.Get()
	rc := rp.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetAll)

	// schedule an event for cathy and george
	testdata.InsertEventFire(rt.DB, testdata.Cathy, testdata.RemindersEvent1, time.Now())
	testdata.InsertEventFire(rt.DB, testdata.George, testdata.RemindersEvent1, time.Now())

	// and george to doctors group, cathy is already part of it
	db.MustExec(`INSERT INTO contacts_contactgroup_contacts(contactgroup_id, contact_id) VALUES($1, $2);`, testdata.DoctorsGroup.ID, testdata.George.ID)

	event := &handler.StopEvent{OrgID: testdata.Org1.ID, ContactID: testdata.Cathy.ID}
	eventJSON, err := json.Marshal(event)
	require.NoError(t, err)
	task := &queue.Task{
		Type:  handler.StopEventType,
		OrgID: int(testdata.Org1.ID),
		Task:  eventJSON,
	}

	err = handler.QueueHandleTask(rc, testdata.Cathy.ID, task)
	assert.NoError(t, err, "error adding task")

	task, err = queue.PopNextTask(rc, queue.HandlerQueue)
	assert.NoError(t, err, "error popping next task")

	err = handler.HandleEvent(ctx, rt, task)
	assert.NoError(t, err, "error when handling event")

	// check that only george is in our group
	assertdb.Query(t, db, `SELECT count(*) from contacts_contactgroup_contacts WHERE contactgroup_id = $1 AND contact_id = $2`, testdata.DoctorsGroup.ID, testdata.Cathy.ID).Returns(0)
	assertdb.Query(t, db, `SELECT count(*) from contacts_contactgroup_contacts WHERE contactgroup_id = $1 AND contact_id = $2`, testdata.DoctorsGroup.ID, testdata.George.ID).Returns(1)

	// that cathy is stopped
	assertdb.Query(t, db, `SELECT count(*) FROM contacts_contact WHERE id = $1 AND status = 'S'`, testdata.Cathy.ID).Returns(1)

	// and has no upcoming events
	assertdb.Query(t, db, `SELECT count(*) FROM campaigns_eventfire WHERE contact_id = $1`, testdata.Cathy.ID).Returns(0)
	assertdb.Query(t, db, `SELECT count(*) FROM campaigns_eventfire WHERE contact_id = $1`, testdata.George.ID).Returns(1)
}

func TestTimedEvents(t *testing.T) {
	ctx, rt, db, rp := testsuite.Get()
	rc := rp.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetAll)

	// start to start our favorites flow
	testdata.InsertKeywordTrigger(db, testdata.Org1, testdata.Favorites, "start", models.MatchOnly, nil, nil)

	tcs := []struct {
		EventType string
		Contact   *testdata.Contact
		Message   string
		Response  string
		ChannelID models.ChannelID
		OrgID     models.OrgID
	}{
		// 0: start the flow
		{handler.MsgEventType, testdata.Cathy, "start", "What is your favorite color?", testdata.TwitterChannel.ID, testdata.Org1.ID},

		// 1: this expiration does nothing because the times don't match
		{handler.ExpirationEventType, testdata.Cathy, "bad", "", testdata.TwitterChannel.ID, testdata.Org1.ID},

		// 2: this checks that the flow wasn't expired
		{handler.MsgEventType, testdata.Cathy, "red", "Good choice, I like Red too! What is your favorite beer?", testdata.TwitterChannel.ID, testdata.Org1.ID},

		// 3: this expiration will actually take
		{handler.ExpirationEventType, testdata.Cathy, "good", "", testdata.TwitterChannel.ID, testdata.Org1.ID},

		// 4: we won't get a response as we will be out of the flow
		{handler.MsgEventType, testdata.Cathy, "mutzig", "", testdata.TwitterChannel.ID, testdata.Org1.ID},

		// 5: start the parent expiration flow
		{handler.MsgEventType, testdata.Cathy, "parent", "Child", testdata.TwitterChannel.ID, testdata.Org1.ID},

		// 6: respond, should bring us out
		{handler.MsgEventType, testdata.Cathy, "hi", "Completed", testdata.TwitterChannel.ID, testdata.Org1.ID},

		// 7: expiring our child should be a no op
		{handler.ExpirationEventType, testdata.Cathy, "child", "", testdata.TwitterChannel.ID, testdata.Org1.ID},

		// 8: respond one last time, should be done
		{handler.MsgEventType, testdata.Cathy, "done", "Ended", testdata.TwitterChannel.ID, testdata.Org1.ID},

		// 9: start our favorite flow again
		{handler.MsgEventType, testdata.Cathy, "start", "What is your favorite color?", testdata.TwitterChannel.ID, testdata.Org1.ID},
	}

	last := time.Now()
	var sessionID models.SessionID
	var runID models.FlowRunID

	for i, tc := range tcs {
		time.Sleep(50 * time.Millisecond)

		var task *queue.Task
		if tc.EventType == handler.MsgEventType {
			event := &handler.MsgEvent{
				ContactID: tc.Contact.ID,
				OrgID:     tc.OrgID,
				ChannelID: tc.ChannelID,
				MsgID:     flows.MsgID(1),
				MsgUUID:   flows.MsgUUID(uuids.New()),
				URN:       tc.Contact.URN,
				URNID:     tc.Contact.URNID,
				Text:      tc.Message,
			}

			eventJSON, err := json.Marshal(event)
			assert.NoError(t, err)

			task = &queue.Task{
				Type:  tc.EventType,
				OrgID: int(tc.OrgID),
				Task:  eventJSON,
			}
		} else if tc.EventType == handler.ExpirationEventType {
			var expiration time.Time

			if tc.Message == "bad" {
				expiration = time.Now()
			} else if tc.Message == "child" {
				db.Get(&expiration, `SELECT wait_expires_on FROM flows_flowsession WHERE id = $1 AND status != 'W'`, sessionID)
				db.Get(&runID, `SELECT id FROM flows_flowrun WHERE session_id = $1 AND status NOT IN ('A', 'W')`, sessionID)
			} else {
				expiration = time.Now().Add(time.Hour * 24)
			}

			task = handler.NewExpirationTask(tc.OrgID, tc.Contact.ID, sessionID, expiration)
		}

		err := handler.QueueHandleTask(rc, tc.Contact.ID, task)
		assert.NoError(t, err, "%d: error adding task", i)

		task, err = queue.PopNextTask(rc, queue.HandlerQueue)
		assert.NoError(t, err, "%d: error popping next task", i)

		err = handler.HandleEvent(ctx, rt, task)
		assert.NoError(t, err, "%d: error when handling event", i)

		if tc.Response != "" {
			assertdb.Query(t, db, `SELECT text FROM msgs_msg WHERE contact_id = $1 AND created_on > $2 ORDER BY id DESC LIMIT 1`, tc.Contact.ID, last).
				Returns(tc.Response, "%d: response: mismatch", i)
		}

		err = db.Get(&sessionID, `SELECT id FROM flows_flowsession WHERE contact_id = $1 ORDER BY created_on DESC LIMIT 1`, tc.Contact.ID)
		assert.NoError(t, err)

		err = db.Get(&runID, `SELECT id FROM flows_flowrun WHERE contact_id = $1 ORDER BY created_on DESC LIMIT 1`, tc.Contact.ID)
		assert.NoError(t, err)

		last = time.Now()
	}

	// should only have a single waiting session/run per contact
	assertdb.Query(t, db, `SELECT count(*) from flows_flowsession WHERE status = 'W' AND contact_id = $1`, testdata.Cathy.ID).Returns(1)
	assertdb.Query(t, db, `SELECT count(*) from flows_flowrun WHERE status = 'W' AND contact_id = $1`, testdata.Cathy.ID).Returns(1)

	// test the case of a run and session no longer being the most recent but somehow still active, expiration should still work
	r, err := db.QueryContext(ctx, `SELECT id, session_id from flows_flowrun WHERE contact_id = $1 and is_active = FALSE order by created_on asc limit 1`, testdata.Cathy.ID)
	assert.NoError(t, err)
	defer r.Close()
	r.Next()
	r.Scan(&runID, &sessionID)

	expiration := time.Now()

	// set both to be active (this requires us to disable the path change trigger for a bit which asserts flows can't cross back into active status)
	db.MustExec(`ALTER TABLE flows_flowrun DISABLE TRIGGER temba_flowrun_path_change`)
	db.MustExec(`UPDATE flows_flowrun SET status = 'W' WHERE id = $1`, runID)
	db.MustExec(`UPDATE flows_flowsession SET status = 'W', wait_started_on = NOW(), wait_expires_on = $2 WHERE id = $1`, sessionID, expiration)
	db.MustExec(`ALTER TABLE flows_flowrun ENABLE TRIGGER temba_flowrun_path_change`)

	// try to expire the run
	task := handler.NewExpirationTask(testdata.Org1.ID, testdata.Cathy.ID, sessionID, expiration)

	err = handler.QueueHandleTask(rc, testdata.Cathy.ID, task)
	assert.NoError(t, err)

	task, err = queue.PopNextTask(rc, queue.HandlerQueue)
	assert.NoError(t, err)

	err = handler.HandleEvent(ctx, rt, task)
	assert.NoError(t, err)
}
