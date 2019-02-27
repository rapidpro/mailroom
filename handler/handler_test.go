package handler

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/utils"
	_ "github.com/nyaruka/mailroom/hooks"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/queue"
	"github.com/nyaruka/mailroom/testsuite"
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
									  flow_id, trigger_type, match_type, created_by_id, modified_by_id, org_id, trigger_count)
		VALUES(TRUE, now(), now(), 'start', false, $1, 'K', 'O', 1, 1, 1, 0) RETURNING id`, models.FavoritesFlowID)

	db.MustExec(
		`INSERT INTO triggers_trigger(is_active, created_on, modified_on, keyword, is_archived, 
									  flow_id, trigger_type, match_type, created_by_id, modified_by_id, org_id, trigger_count)
		VALUES(TRUE, now(), now(), 'start', false, $1, 'K', 'O', 1, 1, 2, 0) RETURNING id`, models.Org2FavoritesFlowID)

	db.MustExec(
		`INSERT INTO triggers_trigger(is_active, created_on, modified_on, keyword, is_archived, 
									  flow_id, trigger_type, match_type, created_by_id, modified_by_id, org_id, trigger_count)
		VALUES(TRUE, now(), now(), '', false, $1, 'C', 'O', 1, 1, 2, 0) RETURNING id`, models.Org2SingleMessageFlowID)

	models.FlushCache()

	tcs := []struct {
		ContactID models.ContactID
		URN       urns.URN
		URNID     models.URNID
		Message   string
		Response  string
		ChannelID models.ChannelID
		OrgID     models.OrgID
	}{
		{models.CathyID, models.CathyURN, models.CathyURNID, "noop", "", models.TwitterChannelID, models.Org1},
		{models.CathyID, models.CathyURN, models.CathyURNID, "start other", "", models.TwitterChannelID, models.Org1},
		{models.CathyID, models.CathyURN, models.CathyURNID, "start", "What is your favorite color?", models.TwitterChannelID, models.Org1},
		{models.CathyID, models.CathyURN, models.CathyURNID, "purple", "I don't know that color. Try again.", models.TwitterChannelID, models.Org1},
		{models.CathyID, models.CathyURN, models.CathyURNID, "blue", "Good choice, I like Blue too! What is your favorite beer?", models.TwitterChannelID, models.Org1},
		{models.CathyID, models.CathyURN, models.CathyURNID, "MUTZIG", "Mmmmm... delicious Mutzig. If only they made blue Mutzig! Lastly, what is your name?", models.TwitterChannelID, models.Org1},
		{models.CathyID, models.CathyURN, models.CathyURNID, "Cathy", "Thanks Cathy, we are all done!", models.TwitterChannelID, models.Org1},
		{models.CathyID, models.CathyURN, models.CathyURNID, "noop", "Thanks Cathy, we are all done!", models.TwitterChannelID, models.Org1},

		{models.Org2FredID, models.Org2FredURN, models.Org2FredURNID, "other", "Hey, how are you?", models.Org2ChannelID, models.Org2},
		{models.Org2FredID, models.Org2FredURN, models.Org2FredURNID, "start", "What is your favorite color?", models.Org2ChannelID, models.Org2},
		{models.Org2FredID, models.Org2FredURN, models.Org2FredURNID, "green", "Good choice, I like Green too! What is your favorite beer?", models.Org2ChannelID, models.Org2},
		{models.Org2FredID, models.Org2FredURN, models.Org2FredURNID, "primus", "Mmmmm... delicious Primus. If only they made green Primus! Lastly, what is your name?", models.Org2ChannelID, models.Org2},
		{models.Org2FredID, models.Org2FredURN, models.Org2FredURNID, "george", "Thanks george, we are all done!", models.Org2ChannelID, models.Org2},
		{models.Org2FredID, models.Org2FredURN, models.Org2FredURNID, "blargh", "Hey, how are you?", models.Org2ChannelID, models.Org2},

		{models.Org2FredID, models.Org2FredURN, models.Org2FredURNID, "start", "What is your favorite color?", models.Org2ChannelID, models.Org2},
	}

	makeMsgTask := func(orgID models.OrgID, channelID models.ChannelID, contactID models.ContactID, urn urns.URN, urnID models.URNID, text string) *queue.Task {
		event := &MsgEvent{
			ContactID: contactID,
			OrgID:     orgID,
			ChannelID: channelID,
			MsgID:     flows.MsgID(1),
			MsgUUID:   flows.MsgUUID(utils.NewUUID()),
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

	for i, tc := range tcs {
		task := makeMsgTask(tc.OrgID, tc.ChannelID, tc.ContactID, tc.URN, tc.URNID, tc.Message)

		err := AddHandleTask(rc, tc.ContactID, task)
		assert.NoError(t, err, "%d: error adding task", i)

		task, err = queue.PopNextTask(rc, queue.HandlerQueue)
		assert.NoError(t, err, "%d: error popping next task", i)

		err = handleContactEvent(ctx, db, rp, task)
		assert.NoError(t, err, "%d: error when handling event", i)

		// if we are meant to have a response
		var text string
		db.Get(&text, `SELECT text FROM msgs_msg WHERE contact_id = $1 ORDER BY id DESC LIMIT 1`, tc.ContactID)
		assert.Equal(t, text, tc.Response, "%d: response: '%s' does not contain '%s'", i, text, tc.Response)
	}

	// force an error by marking our run for fred as complete (our session is still active so this will blow up)
	db.MustExec(`UPDATE flows_flowrun SET is_active = FALSE WHERE contact_id = $1`, models.Org2FredID)
	task := makeMsgTask(models.Org2, models.Org2ChannelID, models.Org2FredID, models.Org2FredURN, models.Org2FredURNID, "red")
	AddHandleTask(rc, models.Org2FredID, task)

	// should get requeued three times automatically, but each should be an error
	for i := 0; i < 3; i++ {
		task, _ = queue.PopNextTask(rc, queue.HandlerQueue)
		assert.NotNil(t, task)
		err := handleContactEvent(ctx, db, rp, task)
		assert.Error(t, err)
	}

	// on third error, no new task
	task, err := queue.PopNextTask(rc, queue.HandlerQueue)
	assert.NoError(t, err)
	assert.Nil(t, task)
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
									  flow_id, trigger_type, match_type, created_by_id, modified_by_id, org_id, trigger_count, channel_id)
		VALUES(TRUE, now(), now(), NULL, false, $1, 'N', NULL, 1, 1, 1, 0, $2) RETURNING id`,
		models.FavoritesFlowID, models.TwitterChannelID)

	// trigger on our nexmo channel for referral and number flow
	db.MustExec(
		`INSERT INTO triggers_trigger(is_active, created_on, modified_on, keyword, is_archived, 
									  flow_id, trigger_type, match_type, created_by_id, modified_by_id, org_id, trigger_count, channel_id)
		VALUES(TRUE, now(), now(), NULL, false, $1, 'R', NULL, 1, 1, 1, 0, $2) RETURNING id`,
		models.PickNumberFlowID, models.NexmoChannelID)

	// add a URN for cathy so we can test twitter URNs
	var cathyTwitterURN models.URNID
	db.Get(&cathyTwitterURN,
		`INSERT INTO contacts_contacturn(identity, path, scheme, priority, contact_id, org_id) 
		                          VALUES('twitterid:123456', '123456', 'twitterid', 10, $1, 1) RETURNING id`,
		models.CathyID)

	tcs := []struct {
		EventType models.ChannelEventType
		ContactID models.ContactID
		URNID     models.URNID
		OrgID     models.OrgID
		ChannelID models.ChannelID
		Extra     map[string]string
		Response  string
	}{
		{NewConversationEventType, models.CathyID, models.CathyURNID, models.Org1, models.TwitterChannelID, nil, "What is your favorite color?"},
		{NewConversationEventType, models.CathyID, models.CathyURNID, models.Org1, models.NexmoChannelID, nil, ""},
		{ReferralEventType, models.CathyID, models.CathyURNID, models.Org1, models.TwitterChannelID, nil, ""},
		{ReferralEventType, models.CathyID, models.CathyURNID, models.Org1, models.NexmoChannelID, nil, "Pick a number between 1-10."},
	}

	models.FlushCache()

	last := time.Now()
	for i, tc := range tcs {
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
		var text string
		err = db.Get(&text, `SELECT text FROM msgs_msg WHERE contact_id = $1 AND contact_urn_id = $2 AND created_on > $3 ORDER BY id DESC LIMIT 1`, tc.ContactID, tc.URNID, last)
		if err != nil {
			logrus.WithError(err).Error("error making query")
		}
		assert.Equal(t, tc.Response, text, "%d: response: '%s' is not '%s'", i, text, tc.Response)
		last = time.Now()
	}
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
									  flow_id, trigger_type, match_type, created_by_id, modified_by_id, org_id, trigger_count)
		VALUES(TRUE, now(), now(), 'start', false, $1, 'K', 'O', 1, 1, 1, 0) RETURNING id`,
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
				MsgUUID:   flows.MsgUUID(utils.NewUUID()),
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
