package handler_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

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
		Hook          func()
		Org           *testdata.Org
		Channel       *testdata.Channel
		Contact       *testdata.Contact
		Text          string
		ExpectedReply string
		ExpectedType  models.MsgType
	}{
		{nil, testdata.Org1, testdata.TwitterChannel, testdata.Cathy, "noop", "", models.MsgTypeInbox},
		{nil, testdata.Org1, testdata.TwitterChannel, testdata.Cathy, "start other", "", models.MsgTypeInbox},
		{nil, testdata.Org1, testdata.TwitterChannel, testdata.Cathy, "start", "What is your favorite color?", models.MsgTypeFlow},
		{nil, testdata.Org1, testdata.TwitterChannel, testdata.Cathy, "purple", "I don't know that color. Try again.", models.MsgTypeFlow},
		{nil, testdata.Org1, testdata.TwitterChannel, testdata.Cathy, "blue", "Good choice, I like Blue too! What is your favorite beer?", models.MsgTypeFlow},
		{nil, testdata.Org1, testdata.TwitterChannel, testdata.Cathy, "MUTZIG", "Mmmmm... delicious Mutzig. If only they made blue Mutzig! Lastly, what is your name?", models.MsgTypeFlow},
		{nil, testdata.Org1, testdata.TwitterChannel, testdata.Cathy, "Cathy", "Thanks Cathy, we are all done!", models.MsgTypeFlow},
		{nil, testdata.Org1, testdata.TwitterChannel, testdata.Cathy, "noop", "", models.MsgTypeInbox},

		{nil, testdata.Org2, testdata.Org2Channel, testdata.Org2Contact, "other", "Hey, how are you?", models.MsgTypeFlow},
		{nil, testdata.Org2, testdata.Org2Channel, testdata.Org2Contact, "start", "What is your favorite color?", models.MsgTypeFlow},
		{nil, testdata.Org2, testdata.Org2Channel, testdata.Org2Contact, "green", "Good choice, I like Green too! What is your favorite beer?", models.MsgTypeFlow},
		{nil, testdata.Org2, testdata.Org2Channel, testdata.Org2Contact, "primus", "Mmmmm... delicious Primus. If only they made green Primus! Lastly, what is your name?", models.MsgTypeFlow},
		{nil, testdata.Org2, testdata.Org2Channel, testdata.Org2Contact, "george", "Thanks george, we are all done!", models.MsgTypeFlow},
		{nil, testdata.Org2, testdata.Org2Channel, testdata.Org2Contact, "blargh", "Hey, how are you?", models.MsgTypeFlow},

		{nil, testdata.Org1, testdata.TwitterChannel, testdata.Bob, "ivr", "", models.MsgTypeFlow},

		// no URN on contact but handle event, session gets started but no message created
		{nil, testdata.Org1, testdata.TwilioChannel, testdata.Alexandria, "start", "", models.MsgTypeFlow},

		// start Fred back in our favorite flow, then make it inactive, will be handled by catch-all
		{nil, testdata.Org2, testdata.Org2Channel, testdata.Org2Contact, "start", "What is your favorite color?", models.MsgTypeFlow},
		{func() {
			db.MustExec(`UPDATE flows_flow SET is_active = FALSE WHERE id = $1`, testdata.Org2Favorites.ID)
		}, testdata.Org2, testdata.Org2Channel, testdata.Org2Contact, "red", "Hey, how are you?", models.MsgTypeFlow},

		// start Fred back in our favorites flow to test retries
		{func() {
			db.MustExec(`UPDATE flows_flow SET is_active = TRUE WHERE id = $1`, testdata.Org2Favorites.ID)
		}, testdata.Org2, testdata.Org2Channel, testdata.Org2Contact, "start", "What is your favorite color?", models.MsgTypeFlow},
	}

	makeMsgTask := func(org *testdata.Org, channel *testdata.Channel, contact *testdata.Contact, text string) *queue.Task {
		event := &handler.MsgEvent{
			ContactID: contact.ID,
			OrgID:     org.ID,
			ChannelID: channel.ID,
			MsgID:     dbMsg.ID(),
			MsgUUID:   dbMsg.UUID(),
			URN:       contact.URN,
			URNID:     contact.URNID,
			Text:      text,
		}

		eventJSON, err := json.Marshal(event)
		assert.NoError(t, err)

		task := &queue.Task{
			Type:  handler.MsgEventType,
			OrgID: int(org.ID),
			Task:  eventJSON,
		}

		return task
	}

	last := time.Now()

	for i, tc := range tcs {
		models.FlushCache()

		// reset our dummy db message into an unhandled state
		db.MustExec(`UPDATE msgs_msg SET status = 'P', msg_type = NULL WHERE id = $1`, dbMsg.ID())

		// run our hook if we have one
		if tc.Hook != nil {
			tc.Hook()
		}

		task := makeMsgTask(tc.Org, tc.Channel, tc.Contact, tc.Text)

		err := handler.QueueHandleTask(rc, tc.Contact.ID, task)
		assert.NoError(t, err, "%d: error adding task", i)

		task, err = queue.PopNextTask(rc, queue.HandlerQueue)
		assert.NoError(t, err, "%d: error popping next task", i)

		err = handler.HandleEvent(ctx, rt, task)
		assert.NoError(t, err, "%d: error when handling event", i)

		// check that message is marked as handled with expected type
		testsuite.AssertQuery(t, db, `SELECT msg_type, status FROM msgs_msg WHERE id = $1`, dbMsg.ID()).
			Columns(map[string]interface{}{"msg_type": string(tc.ExpectedType), "status": "H"}, "%d: msg state mismatch", i)

		// if we are meant to have a reply, check it
		if tc.ExpectedReply != "" {
			testsuite.AssertQuery(t, db, `SELECT text FROM msgs_msg WHERE contact_id = $1 AND created_on > $2 ORDER BY id DESC LIMIT 1`, tc.Contact.ID, last).
				Returns(tc.ExpectedReply, "%d: response mismatch", i)
		}

		// check any open tickets for this contact where updated
		numOpenTickets := len(openTickets[tc.Contact])
		testsuite.AssertQuery(t, db, `SELECT count(*) FROM tickets_ticket WHERE contact_id = $1 AND status = 'O' AND last_activity_on > $2`, tc.Contact.ID, last).
			Returns(numOpenTickets, "%d: updated open ticket mismatch", i)

		// check any closed tickets are unchanged
		numClosedTickets := len(closedTickets[tc.Contact])
		testsuite.AssertQuery(t, db, `SELECT count(*) FROM tickets_ticket WHERE contact_id = $1 AND status = 'C' AND last_activity_on = '2021-01-01T00:00:00Z'`, tc.Contact.ID).
			Returns(numClosedTickets, "%d: unchanged closed ticket mismatch", i)

		last = time.Now()
	}

	// should have one remaining IVR task to handle for Bob
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
	testsuite.AssertQuery(t, db, `SELECT count(*) from flows_flowsession where contact_id = $1 and timeout_on IS NULL AND wait_started_on IS NOT NULL`, testdata.Org2Contact.ID).Returns(2)

	// force an error by marking our run for fred as complete (our session is still active so this will blow up)
	db.MustExec(`UPDATE flows_flowrun SET is_active = FALSE WHERE contact_id = $1`, testdata.Org2Contact.ID)
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
	testsuite.AssertQuery(t, db, `SELECT text FROM msgs_msg WHERE contact_id = $1 AND direction = 'O' ORDER BY id DESC LIMIT 1`, testdata.Org2Contact.ID).Returns("Hey, how are you?")
	previous := time.Now()

	// and should have failed previous session
	testsuite.AssertQuery(t, db, `SELECT count(*) from flows_flowsession where contact_id = $1 and status = 'F' and current_flow_id = $2`, testdata.Org2Contact.ID, testdata.Org2Favorites.ID).Returns(2)

	// trigger should also not start a new session
	task = makeMsgTask(testdata.Org2, testdata.Org2Channel, testdata.Org2Contact, "start")
	handler.QueueHandleTask(rc, testdata.Org2Contact.ID, task)
	task, _ = queue.PopNextTask(rc, queue.HandlerQueue)
	err = handler.HandleEvent(ctx, rt, task)
	assert.NoError(t, err)

	testsuite.AssertQuery(t, db, `SELECT count(*) FROM msgs_msg WHERE contact_id = $1 AND direction = 'O' AND created_on > $2`, testdata.Org2Contact.ID, previous).Returns(0)
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
			testsuite.AssertQuery(t, db, `SELECT text FROM msgs_msg WHERE contact_id = $1 AND contact_urn_id = $2 AND created_on > $3 ORDER BY id DESC LIMIT 1`, tc.ContactID, tc.URNID, start).
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

	testsuite.AssertQuery(t, rt.DB, `SELECT count(*) FROM msgs_msg WHERE contact_id = $1 AND direction = 'O' AND text = 'What is your favorite color?'`, testdata.Cathy.ID).Returns(1)
}

func TestStopEvent(t *testing.T) {
	ctx, rt, db, rp := testsuite.Get()
	rc := rp.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetAll)

	// schedule an event for cathy and george
	db.MustExec(`INSERT INTO campaigns_eventfire(scheduled, contact_id, event_id) VALUES (NOW(), $1, $3), (NOW(), $2, $3);`, testdata.Cathy.ID, testdata.George.ID, testdata.RemindersEvent1.ID)

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
	testsuite.AssertQuery(t, db, `SELECT count(*) from contacts_contactgroup_contacts WHERE contactgroup_id = $1 AND contact_id = $2`, testdata.DoctorsGroup.ID, testdata.Cathy.ID).Returns(0)
	testsuite.AssertQuery(t, db, `SELECT count(*) from contacts_contactgroup_contacts WHERE contactgroup_id = $1 AND contact_id = $2`, testdata.DoctorsGroup.ID, testdata.George.ID).Returns(1)

	// that cathy is stopped
	testsuite.AssertQuery(t, db, `SELECT count(*) FROM contacts_contact WHERE id = $1 AND status = 'S'`, testdata.Cathy.ID).Returns(1)

	// and has no upcoming events
	testsuite.AssertQuery(t, db, `SELECT count(*) FROM campaigns_eventfire WHERE contact_id = $1`, testdata.Cathy.ID).Returns(0)
	testsuite.AssertQuery(t, db, `SELECT count(*) FROM campaigns_eventfire WHERE contact_id = $1`, testdata.George.ID).Returns(1)
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
	var runExpiration *time.Time

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
				db.Get(&expiration, `SELECT expires_on FROM flows_flowrun WHERE session_id = $1 AND is_active = FALSE`, sessionID)
				db.Get(&runID, `SELECT id FROM flows_flowrun WHERE session_id = $1 AND is_active = FALSE`, sessionID)
			} else if runExpiration != nil {
				expiration = *runExpiration
			} else {
				// exited runs no longer have expiration set so just fake a value - the task will ignore inactive runs anyway
				expiration = time.Now().Add(time.Hour * 24)
			}

			task = handler.NewExpirationTask(
				tc.OrgID,
				tc.Contact.ID,
				sessionID,
				runID,
				expiration,
			)
		}

		err := handler.QueueHandleTask(rc, tc.Contact.ID, task)
		assert.NoError(t, err, "%d: error adding task", i)

		task, err = queue.PopNextTask(rc, queue.HandlerQueue)
		assert.NoError(t, err, "%d: error popping next task", i)

		err = handler.HandleEvent(ctx, rt, task)
		assert.NoError(t, err, "%d: error when handling event", i)

		if tc.Response != "" {
			testsuite.AssertQuery(t, db, `SELECT text FROM msgs_msg WHERE contact_id = $1 AND created_on > $2 ORDER BY id DESC LIMIT 1`, tc.Contact.ID, last).
				Returns(tc.Response, "%d: response: mismatch", i)
		}

		err = db.Get(&sessionID, `SELECT id FROM flows_flowsession WHERE contact_id = $1 ORDER BY created_on DESC LIMIT 1`, tc.Contact.ID)
		assert.NoError(t, err)

		err = db.Get(&runID, `SELECT id FROM flows_flowrun WHERE contact_id = $1 ORDER BY created_on DESC LIMIT 1`, tc.Contact.ID)
		assert.NoError(t, err)

		err = db.Get(&runExpiration, `SELECT expires_on FROM flows_flowrun WHERE contact_id = $1 ORDER BY created_on DESC LIMIT 1`, tc.Contact.ID)
		assert.NoError(t, err)

		last = time.Now()
	}

	// test the case of a run and session no longer being the most recent but somehow still active, expiration should still work
	r, err := db.QueryContext(ctx, `SELECT id, session_id from flows_flowrun WHERE contact_id = $1 and is_active = FALSE order by created_on asc limit 1`, testdata.Cathy.ID)
	assert.NoError(t, err)
	defer r.Close()
	r.Next()
	r.Scan(&runID, &sessionID)

	expiration := time.Now()

	// set both to be active (this requires us to disable the path change trigger for a bit which asserts flows can't cross back into active status)
	db.MustExec(`ALTER TABLE flows_flowrun DISABLE TRIGGER temba_flowrun_path_change`)
	db.MustExec(`UPDATE flows_flowrun SET is_active = TRUE, status = 'W', expires_on = $2 WHERE id = $1`, runID, expiration)
	db.MustExec(`UPDATE flows_flowsession SET status = 'W' WHERE id = $1`, sessionID)
	db.MustExec(`ALTER TABLE flows_flowrun ENABLE TRIGGER temba_flowrun_path_change`)

	// try to expire the run
	task := handler.NewExpirationTask(
		testdata.Org1.ID,
		testdata.Cathy.ID,
		sessionID,
		runID,
		expiration,
	)

	err = handler.QueueHandleTask(rc, testdata.Cathy.ID, task)
	assert.NoError(t, err)

	task, err = queue.PopNextTask(rc, queue.HandlerQueue)
	assert.NoError(t, err)

	err = handler.HandleEvent(ctx, rt, task)
	assert.NoError(t, err)

	testsuite.AssertQuery(t, db, `SELECT count(*) from flows_flowrun WHERE is_active = FALSE AND status = 'F' AND id = $1`, runID).Returns(1)
	testsuite.AssertQuery(t, db, `SELECT count(*) from flows_flowsession WHERE status = 'F' AND id = $1`, sessionID).Returns(1)
}
