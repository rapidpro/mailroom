package handler

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/flows"
	_ "github.com/nyaruka/mailroom/hooks"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/queue"
	"github.com/nyaruka/mailroom/testsuite"

	"github.com/gomodule/redigo/redis"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestMsgEvents(t *testing.T) {
	testsuite.Reset()
	db := testsuite.DB()
	rp := testsuite.RP()
	ctx := testsuite.CTX()

	rc := rp.Get()
	defer rc.Close()

	db.MustExec(
		`INSERT INTO triggers_trigger(is_active, created_on, modified_on, keyword, is_archived, 
									  flow_id, trigger_type, match_type, created_by_id, modified_by_id, org_id)
		VALUES(TRUE, now(), now(), 'start', false, $1, 'K', 'O', 1, 1, 1) RETURNING id`, models.FavoritesFlowID)

	db.MustExec(
		`INSERT INTO triggers_trigger(is_active, created_on, modified_on, keyword, is_archived, 
									  flow_id, trigger_type, match_type, created_by_id, modified_by_id, org_id)
		VALUES(TRUE, now(), now(), 'start', false, $1, 'K', 'O', 1, 1, 2) RETURNING id`, models.Org2FavoritesFlowID)

	db.MustExec(
		`INSERT INTO triggers_trigger(is_active, created_on, modified_on, keyword, is_archived, 
									  flow_id, trigger_type, match_type, created_by_id, modified_by_id, org_id)
		VALUES(TRUE, now(), now(), '', false, $1, 'C', 'O', 1, 1, 2) RETURNING id`, models.Org2SingleMessageFlowID)

	db.MustExec(
		`INSERT INTO triggers_trigger(is_active, created_on, modified_on, keyword, is_archived, 
									  flow_id, trigger_type, match_type, created_by_id, modified_by_id, org_id)
		VALUES(TRUE, now(), now(), 'ivr', false, $1, 'K', 'O', 1, 1, 1) RETURNING id`, models.IVRFlowID)

	// clear all of Alexandria's URNs
	db.MustExec(`UPDATE contacts_contacturn SET contact_id = NULL WHERE contact_id = $1`, models.AlexandriaID)

	models.FlushCache()

	tcs := []struct {
		Hook      func()
		ContactID models.ContactID
		URN       urns.URN
		URNID     models.URNID
		Message   string
		Response  string
		ChannelID models.ChannelID
		OrgID     models.OrgID
	}{
		{nil, models.CathyID, models.CathyURN, models.CathyURNID, "noop", "", models.TwitterChannelID, models.Org1},
		{nil, models.CathyID, models.CathyURN, models.CathyURNID, "start other", "", models.TwitterChannelID, models.Org1},
		{nil, models.CathyID, models.CathyURN, models.CathyURNID, "start", "What is your favorite color?", models.TwitterChannelID, models.Org1},
		{nil, models.CathyID, models.CathyURN, models.CathyURNID, "purple", "I don't know that color. Try again.", models.TwitterChannelID, models.Org1},
		{nil, models.CathyID, models.CathyURN, models.CathyURNID, "blue", "Good choice, I like Blue too! What is your favorite beer?", models.TwitterChannelID, models.Org1},
		{nil, models.CathyID, models.CathyURN, models.CathyURNID, "MUTZIG", "Mmmmm... delicious Mutzig. If only they made blue Mutzig! Lastly, what is your name?", models.TwitterChannelID, models.Org1},
		{nil, models.CathyID, models.CathyURN, models.CathyURNID, "Cathy", "Thanks Cathy, we are all done!", models.TwitterChannelID, models.Org1},
		{nil, models.CathyID, models.CathyURN, models.CathyURNID, "noop", "", models.TwitterChannelID, models.Org1},

		{nil, models.Org2FredID, models.Org2FredURN, models.Org2FredURNID, "other", "Hey, how are you?", models.Org2ChannelID, models.Org2},
		{nil, models.Org2FredID, models.Org2FredURN, models.Org2FredURNID, "start", "What is your favorite color?", models.Org2ChannelID, models.Org2},
		{nil, models.Org2FredID, models.Org2FredURN, models.Org2FredURNID, "green", "Good choice, I like Green too! What is your favorite beer?", models.Org2ChannelID, models.Org2},
		{nil, models.Org2FredID, models.Org2FredURN, models.Org2FredURNID, "primus", "Mmmmm... delicious Primus. If only they made green Primus! Lastly, what is your name?", models.Org2ChannelID, models.Org2},
		{nil, models.Org2FredID, models.Org2FredURN, models.Org2FredURNID, "george", "Thanks george, we are all done!", models.Org2ChannelID, models.Org2},
		{nil, models.Org2FredID, models.Org2FredURN, models.Org2FredURNID, "blargh", "Hey, how are you?", models.Org2ChannelID, models.Org2},

		{nil, models.BobID, models.BobURN, models.BobURNID, "ivr", "", models.TwitterChannelID, models.Org1},

		// no URN on contact but handle event, session gets started but no message created
		{nil, models.AlexandriaID, models.AlexandriaURN, models.AlexandriaURNID, "start", "", models.TwilioChannelID, models.Org1},

		// start Fred back in our favorite flow, then make it inactive, will be handled by catch-all
		{nil, models.Org2FredID, models.Org2FredURN, models.Org2FredURNID, "start", "What is your favorite color?", models.Org2ChannelID, models.Org2},
		{func() {
			db.MustExec(`UPDATE flows_flow SET is_active = FALSE WHERE id = $1`, models.Org2FavoritesFlowID)
		}, models.Org2FredID, models.Org2FredURN, models.Org2FredURNID, "red", "Hey, how are you?", models.Org2ChannelID, models.Org2},

		// start Fred back in our favorites flow to test retries
		{func() {
			db.MustExec(`UPDATE flows_flow SET is_active = TRUE WHERE id = $1`, models.Org2FavoritesFlowID)
		}, models.Org2FredID, models.Org2FredURN, models.Org2FredURNID, "start", "What is your favorite color?", models.Org2ChannelID, models.Org2},
	}

	makeMsgTask := func(orgID models.OrgID, channelID models.ChannelID, contactID models.ContactID, urn urns.URN, urnID models.URNID, text string) *queue.Task {
		event := &MsgEvent{
			ContactID: contactID,
			OrgID:     orgID,
			ChannelID: channelID,
			MsgID:     flows.MsgID(1),
			MsgUUID:   flows.MsgUUID(uuids.New()),
			URN:       urn,
			URNID:     urnID,
			Text:      text,
		}

		eventJSON, err := json.Marshal(event)
		assert.NoError(t, err)

		task := &queue.Task{
			Type:  MsgEventType,
			OrgID: int(orgID),
			Task:  eventJSON,
		}

		return task
	}

	last := time.Now()

	for i, tc := range tcs {
		models.FlushCache()

		// run our hook if we have one
		if tc.Hook != nil {
			tc.Hook()
		}

		task := makeMsgTask(tc.OrgID, tc.ChannelID, tc.ContactID, tc.URN, tc.URNID, tc.Message)

		err := AddHandleTask(rc, tc.ContactID, task)
		assert.NoError(t, err, "%d: error adding task", i)

		task, err = queue.PopNextTask(rc, queue.HandlerQueue)
		assert.NoError(t, err, "%d: error popping next task", i)

		err = handleContactEvent(ctx, db, rp, task)
		assert.NoError(t, err, "%d: error when handling event", i)

		// if we are meant to have a response
		var text string
		db.Get(&text, `SELECT text FROM msgs_msg WHERE contact_id = $1 AND created_on > $2 ORDER BY id DESC LIMIT 1`, tc.ContactID, last)
		assert.Equal(t, text, tc.Response, "%d: response: '%s' does not contain '%s'", i, text, tc.Response)

		last = time.Now()
	}

	// should have one remaining IVR task to handle for Bob
	task, err := queue.PopNextTask(rc, queue.BatchQueue)
	assert.NoError(t, err)
	assert.NotNil(t, task)
	assert.Equal(t, queue.StartIVRFlowBatch, task.Type)

	// should have 7 queued priority messages
	count, err := redis.Int(rc.Do("zcard", fmt.Sprintf("msgs:%s|10/1", models.Org2ChannelUUID)))
	assert.NoError(t, err)
	assert.Equal(t, 9, count)

	// Fred's sessions should not have a timeout because courier will set them
	testsuite.AssertQueryCount(t, db,
		`SELECT count(*) from flows_flowsession where contact_id = $1 and timeout_on IS NULL AND wait_started_on IS NOT NULL`,
		[]interface{}{models.Org2FredID}, 2,
	)

	// force an error by marking our run for fred as complete (our session is still active so this will blow up)
	db.MustExec(`UPDATE flows_flowrun SET is_active = FALSE WHERE contact_id = $1`, models.Org2FredID)
	task = makeMsgTask(models.Org2, models.Org2ChannelID, models.Org2FredID, models.Org2FredURN, models.Org2FredURNID, "red")
	AddHandleTask(rc, models.Org2FredID, task)

	// should get requeued three times automatically
	for i := 0; i < 3; i++ {
		task, _ = queue.PopNextTask(rc, queue.HandlerQueue)
		assert.NotNil(t, task)
		err := handleContactEvent(ctx, db, rp, task)
		assert.NoError(t, err)
	}

	// on third error, no new task
	task, err = queue.PopNextTask(rc, queue.HandlerQueue)
	assert.NoError(t, err)
	assert.Nil(t, task)

	// mark Fred's flow as inactive
	db.MustExec(`UPDATE flows_flow SET is_active = FALSE where id = $1`, models.Org2FavoritesFlowID)
	models.FlushCache()

	// try to resume now
	task = makeMsgTask(models.Org2, models.Org2ChannelID, models.Org2FredID, models.Org2FredURN, models.Org2FredURNID, "red")
	AddHandleTask(rc, models.Org2FredID, task)
	task, _ = queue.PopNextTask(rc, queue.HandlerQueue)
	assert.NotNil(t, task)
	err = handleContactEvent(ctx, db, rp, task)
	assert.NoError(t, err)

	// should get our catch all trigger
	text := ""
	db.Get(&text, `SELECT text FROM msgs_msg WHERE contact_id = $1 AND direction = 'O' ORDER BY id DESC LIMIT 1`, models.Org2FredID)
	assert.Equal(t, "Hey, how are you?", text)
	previous := time.Now()

	// and should have interrupted previous session
	testsuite.AssertQueryCount(t, db,
		`SELECT count(*) from flows_flowsession where contact_id = $1 and status ='I' and current_flow_id = $2`,
		[]interface{}{models.Org2FredID, models.Org2FavoritesFlowID}, 2,
	)

	// trigger should also not start a new session
	task = makeMsgTask(models.Org2, models.Org2ChannelID, models.Org2FredID, models.Org2FredURN, models.Org2FredURNID, "start")
	AddHandleTask(rc, models.Org2FredID, task)
	task, _ = queue.PopNextTask(rc, queue.HandlerQueue)
	err = handleContactEvent(ctx, db, rp, task)
	assert.NoError(t, err)

	db.Get(&text, `SELECT text FROM msgs_msg WHERE contact_id = $1 AND direction = 'O' AND created_on > $2 ORDER BY id DESC LIMIT 1`, models.Org2FredID, previous)
	assert.Equal(t, "Hey, how are you?", text)
}

func TestChannelEvents(t *testing.T) {
	testsuite.Reset()
	db := testsuite.DB()
	rp := testsuite.RP()
	ctx := testsuite.CTX()

	rc := rp.Get()
	defer rc.Close()

	logrus.Info("starting channel test")

	// trigger on our twitter channel for new conversations and favorites flow
	db.MustExec(
		`INSERT INTO triggers_trigger(is_active, created_on, modified_on, keyword, is_archived, 
									  flow_id, trigger_type, match_type, created_by_id, modified_by_id, org_id, channel_id)
		VALUES(TRUE, now(), now(), NULL, false, $1, 'N', NULL, 1, 1, 1, $2) RETURNING id`,
		models.FavoritesFlowID, models.TwitterChannelID)

	// trigger on our nexmo channel for referral and number flow
	db.MustExec(
		`INSERT INTO triggers_trigger(is_active, created_on, modified_on, keyword, is_archived, 
									  flow_id, trigger_type, match_type, created_by_id, modified_by_id, org_id, channel_id)
		VALUES(TRUE, now(), now(), NULL, false, $1, 'R', NULL, 1, 1, 1, $2) RETURNING id`,
		models.PickNumberFlowID, models.NexmoChannelID)

	// add a URN for cathy so we can test twitter URNs
	var cathyTwitterURN models.URNID
	db.Get(&cathyTwitterURN,
		`INSERT INTO contacts_contacturn(identity, path, scheme, priority, contact_id, org_id) 
		                          VALUES('twitterid:123456', '123456', 'twitterid', 10, $1, 1) RETURNING id`,
		models.CathyID)

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
		{NewConversationEventType, models.CathyID, models.CathyURNID, models.Org1, models.TwitterChannelID, nil, "What is your favorite color?", true},
		{NewConversationEventType, models.CathyID, models.CathyURNID, models.Org1, models.NexmoChannelID, nil, "", true},
		{WelcomeMessageEventType, models.CathyID, models.CathyURNID, models.Org1, models.NexmoChannelID, nil, "", false},
		{ReferralEventType, models.CathyID, models.CathyURNID, models.Org1, models.TwitterChannelID, nil, "", true},
		{ReferralEventType, models.CathyID, models.CathyURNID, models.Org1, models.NexmoChannelID, nil, "Pick a number between 1-10.", true},
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

		err = AddHandleTask(rc, tc.ContactID, task)
		assert.NoError(t, err, "%d: error adding task", i)

		task, err = queue.PopNextTask(rc, queue.HandlerQueue)
		assert.NoError(t, err, "%d: error popping next task", i)

		err = handleContactEvent(ctx, db, rp, task)
		assert.NoError(t, err, "%d: error when handling event", i)

		// if we are meant to have a response
		if tc.Response != "" {
			var text string
			err = db.Get(&text, `SELECT text FROM msgs_msg WHERE contact_id = $1 AND contact_urn_id = $2 AND created_on > $3 ORDER BY id DESC LIMIT 1`, tc.ContactID, tc.URNID, start)
			assert.NoError(t, err)
			assert.Equal(t, tc.Response, text, "%d: response: '%s' is not '%s'", i, text, tc.Response)
		}

		if tc.UpdateLastSeen {
			var lastSeen time.Time
			err = db.Get(&lastSeen, `SELECT last_seen_on FROM contacts_contact WHERE id = $1`, tc.ContactID)
			assert.NoError(t, err)
			assert.True(t, lastSeen.Equal(start) || lastSeen.After(start), "%d: expected last seen to be updated", i)
		}
	}
}

func TestStopEvent(t *testing.T) {
	testsuite.Reset()
	db := testsuite.DB()
	rp := testsuite.RP()
	ctx := testsuite.CTX()

	rc := rp.Get()
	defer rc.Close()

	// schedule an event for cathy and george
	db.MustExec(`INSERT INTO campaigns_eventfire(scheduled, contact_id, event_id) VALUES (NOW(), $1, $3), (NOW(), $2, $3);`, models.CathyID, models.GeorgeID, models.RemindersEvent1ID)

	// and george to doctors group, cathy is already part of it
	db.MustExec(`INSERT INTO contacts_contactgroup_contacts(contactgroup_id, contact_id) VALUES($1, $2);`, models.DoctorsGroupID, models.GeorgeID)

	event := &StopEvent{OrgID: models.Org1, ContactID: models.CathyID}
	eventJSON, err := json.Marshal(event)
	task := &queue.Task{
		Type:  StopEventType,
		OrgID: int(models.Org1),
		Task:  eventJSON,
	}

	err = AddHandleTask(rc, models.CathyID, task)
	assert.NoError(t, err, "error adding task")

	task, err = queue.PopNextTask(rc, queue.HandlerQueue)
	assert.NoError(t, err, "error popping next task")

	err = handleContactEvent(ctx, db, rp, task)
	assert.NoError(t, err, "error when handling event")

	// check that only george is in our group
	testsuite.AssertQueryCount(t, db, `SELECT count(*) from contacts_contactgroup_contacts WHERE contactgroup_id = $1 AND contact_id = $2`, []interface{}{models.DoctorsGroupID, models.CathyID}, 0)
	testsuite.AssertQueryCount(t, db, `SELECT count(*) from contacts_contactgroup_contacts WHERE contactgroup_id = $1 AND contact_id = $2`, []interface{}{models.DoctorsGroupID, models.GeorgeID}, 1)

	// that cathy is stopped
	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM contacts_contact WHERE id = $1 AND status = 'S'`, []interface{}{models.CathyID}, 1)

	// and has no upcoming events
	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM campaigns_eventfire WHERE contact_id = $1`, []interface{}{models.CathyID}, 0)
	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM campaigns_eventfire WHERE contact_id = $1`, []interface{}{models.GeorgeID}, 1)
}

func TestTimedEvents(t *testing.T) {
	testsuite.Reset()
	db := testsuite.DB()
	rp := testsuite.RP()
	ctx := testsuite.CTX()

	rc := rp.Get()
	defer rc.Close()

	// start to start our favorites flow
	db.MustExec(
		`INSERT INTO triggers_trigger(is_active, created_on, modified_on, keyword, is_archived, 
									  flow_id, trigger_type, match_type, created_by_id, modified_by_id, org_id)
		VALUES(TRUE, now(), now(), 'start', false, $1, 'K', 'O', 1, 1, 1) RETURNING id`,
		models.FavoritesFlowID,
	)

	models.FlushCache()

	tcs := []struct {
		EventType string
		ContactID models.ContactID
		URN       urns.URN
		URNID     models.URNID
		Message   string
		Response  string
		ChannelID models.ChannelID
		OrgID     models.OrgID
	}{
		// start the flow
		{MsgEventType, models.CathyID, models.CathyURN, models.CathyURNID, "start", "What is your favorite color?", models.TwitterChannelID, models.Org1},

		// this expiration does nothing because the times don't match
		{ExpirationEventType, models.CathyID, models.CathyURN, models.CathyURNID, "bad", "", models.TwitterChannelID, models.Org1},

		// this checks that the flow wasn't expired
		{MsgEventType, models.CathyID, models.CathyURN, models.CathyURNID, "red", "Good choice, I like Red too! What is your favorite beer?", models.TwitterChannelID, models.Org1},

		// this expiration will actually take
		{ExpirationEventType, models.CathyID, models.CathyURN, models.CathyURNID, "good", "", models.TwitterChannelID, models.Org1},

		// we won't get a response as we will be out of the flow
		{MsgEventType, models.CathyID, models.CathyURN, models.CathyURNID, "mutzig", "", models.TwitterChannelID, models.Org1},

		// start the parent expiration flow
		{MsgEventType, models.CathyID, models.CathyURN, models.CathyURNID, "parent", "Child", models.TwitterChannelID, models.Org1},

		// respond, should bring us out
		{MsgEventType, models.CathyID, models.CathyURN, models.CathyURNID, "hi", "Completed", models.TwitterChannelID, models.Org1},

		// expiring our child should be a no op
		{ExpirationEventType, models.CathyID, models.CathyURN, models.CathyURNID, "child", "", models.TwitterChannelID, models.Org1},

		// respond one last time, should be done
		{MsgEventType, models.CathyID, models.CathyURN, models.CathyURNID, "done", "Ended", models.TwitterChannelID, models.Org1},
	}

	last := time.Now()
	var sessionID models.SessionID
	var runID models.FlowRunID
	var runExpiration time.Time

	for i, tc := range tcs {
		time.Sleep(50 * time.Millisecond)

		var task *queue.Task
		if tc.EventType == MsgEventType {
			event := &MsgEvent{
				ContactID: tc.ContactID,
				OrgID:     tc.OrgID,
				ChannelID: tc.ChannelID,
				MsgID:     flows.MsgID(1),
				MsgUUID:   flows.MsgUUID(uuids.New()),
				URN:       tc.URN,
				URNID:     tc.URNID,
				Text:      tc.Message,
			}

			eventJSON, err := json.Marshal(event)
			assert.NoError(t, err)

			task = &queue.Task{
				Type:  tc.EventType,
				OrgID: int(tc.OrgID),
				Task:  eventJSON,
			}
		} else if tc.EventType == ExpirationEventType {
			var expiration time.Time
			if tc.Message == "bad" {
				expiration = time.Now()
			} else if tc.Message == "child" {
				db.Get(&expiration, `SELECT expires_on FROM flows_flowrun WHERE session_id = $1 AND is_active = FALSE`, sessionID)
				db.Get(&runID, `SELECT id FROM flows_flowrun WHERE session_id = $1 AND is_active = FALSE`, sessionID)
			} else {
				expiration = runExpiration
			}

			task = newTimedTask(
				ExpirationEventType,
				tc.OrgID,
				tc.ContactID,
				sessionID,
				runID,
				expiration,
			)
		}

		err := AddHandleTask(rc, tc.ContactID, task)
		assert.NoError(t, err, "%d: error adding task", i)

		task, err = queue.PopNextTask(rc, queue.HandlerQueue)
		assert.NoError(t, err, "%d: error popping next task", i)

		err = handleContactEvent(ctx, db, rp, task)
		assert.NoError(t, err, "%d: error when handling event", i)

		var text string
		db.Get(&text, `SELECT text FROM msgs_msg WHERE contact_id = $1 AND created_on > $2 ORDER BY id DESC LIMIT 1`, tc.ContactID, last)
		assert.Equal(t, text, tc.Response, "%d: response: '%s' does not match '%s'", i, text, tc.Response)

		err = db.Get(&sessionID, `SELECT id FROM flows_flowsession WHERE contact_id = $1 ORDER BY created_on DESC LIMIT 1`, tc.ContactID)
		assert.NoError(t, err)

		err = db.Get(&runID, `SELECT id FROM flows_flowrun WHERE contact_id = $1 ORDER BY created_on DESC LIMIT 1`, tc.ContactID)
		assert.NoError(t, err)

		err = db.Get(&runExpiration, `SELECT expires_on FROM flows_flowrun WHERE contact_id = $1 ORDER BY created_on DESC LIMIT 1`, tc.ContactID)
		assert.NoError(t, err)

		last = time.Now()
	}
}
