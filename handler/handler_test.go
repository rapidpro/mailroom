package handler

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom"
	_ "github.com/nyaruka/mailroom/hooks"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/queue"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/stretchr/testify/assert"
)

const (
	org1           = models.OrgID(1)
	cathy          = flows.ContactID(42)
	cathyURN       = urns.URN("tel:+250700000001")
	cathyURNID     = models.URNID(42)
	twitterChannel = models.ChannelID(3)
	nexmoChannel   = models.ChannelID(2)

	org2        = models.OrgID(2)
	george      = flows.ContactID(54)
	georgeURN   = urns.URN("tel:+250700000013")
	georgeURNID = models.URNID(55)
	channel2    = models.ChannelID(5)

	favoritesFlow = models.FlowID(1)
	numberFlow    = models.FlowID(3)
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
		VALUES(TRUE, now(), now(), 'start', false, 1, 'K', 'O', 1, 1, 1, 0) RETURNING id`)

	db.MustExec(
		`INSERT INTO triggers_trigger(is_active, created_on, modified_on, keyword, is_archived, 
									  flow_id, trigger_type, match_type, created_by_id, modified_by_id, org_id, trigger_count)
		VALUES(TRUE, now(), now(), 'start', false, 1, 'K', 'O', 1, 1, 2, 0) RETURNING id`)

	db.MustExec(
		`INSERT INTO triggers_trigger(is_active, created_on, modified_on, keyword, is_archived, 
									  flow_id, trigger_type, match_type, created_by_id, modified_by_id, org_id, trigger_count)
		VALUES(TRUE, now(), now(), '', false, 32, 'C', 'O', 1, 1, 2, 0) RETURNING id`)

	tcs := []struct {
		ContactID flows.ContactID
		URN       urns.URN
		URNID     models.URNID
		Message   string
		Response  string
		ChannelID models.ChannelID
		OrgID     models.OrgID
	}{
		{cathy, cathyURN, cathyURNID, "noop", "", twitterChannel, org1},
		{cathy, cathyURN, cathyURNID, "start other", "", twitterChannel, org1},
		{cathy, cathyURN, cathyURNID, "start", "What is your favorite color?", twitterChannel, org1},
		{cathy, cathyURN, cathyURNID, "purple", "I don't know that color. Try again.", twitterChannel, org1},
		{cathy, cathyURN, cathyURNID, "blue", "Good choice, I like Blue too! What is your favorite beer?", twitterChannel, org1},
		{cathy, cathyURN, cathyURNID, "MUTZIG", "Mmmmm... delicious Mutzig. If only they made blue Mutzig! Lastly, what is your name?", twitterChannel, org1},
		{cathy, cathyURN, cathyURNID, "Cathy", "Thanks Cathy, we are all done!", twitterChannel, org1},
		{cathy, cathyURN, cathyURNID, "noop", "Thanks Cathy, we are all done!", twitterChannel, org1},

		{george, georgeURN, georgeURNID, "other", "Hi George Newman, it is time to consult with your patients.", channel2, org2},
		{george, georgeURN, georgeURNID, "start", "What is your favorite color?", channel2, org2},
		{george, georgeURN, georgeURNID, "green", "Good choice, I like Green too! What is your favorite beer?", channel2, org2},
		{george, georgeURN, georgeURNID, "primus", "Mmmmm... delicious Primus. If only they made green Primus! Lastly, what is your name?", channel2, org2},
		{george, georgeURN, georgeURNID, "george", "Thanks george, we are all done!", channel2, org2},
		{george, georgeURN, georgeURNID, "blargh", "Hi George Newman, it is time to consult with your patients.", channel2, org2},
	}

	for i, tc := range tcs {
		event := &msgEvent{
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

		task := &queue.Task{
			Type:  msgEventType,
			OrgID: int(tc.OrgID),
			Task:  eventJSON,
		}

		err = AddHandleTask(rc, tc.ContactID, task)
		assert.NoError(t, err, "%d: error adding task", i)

		task, err = queue.PopNextTask(rc, mailroom.HandlerQueue)
		assert.NoError(t, err, "%d: error popping next task", i)

		err = handleContactEvent(ctx, db, rp, task)
		assert.NoError(t, err, "%d: error when handling event", i)

		// if we are meant to have a response
		if tc.Response != "" {
			var text string
			err := db.Get(&text, `SELECT text FROM msgs_msg WHERE contact_id = $1 ORDER BY id DESC LIMIT 1`, tc.ContactID)
			if err != nil {
				assert.NoError(t, err, "%d: error selecting last message", i)
				continue
			}

			assert.True(t, strings.Contains(text, tc.Response), "%d: response: '%s' does not contain '%s'", i, text, tc.Response)
		}
	}
}

func TestChannelEvents(t *testing.T) {
	testsuite.Reset()
	db := testsuite.DB()
	rp := testsuite.RP()
	ctx := testsuite.CTX()

	rc := rp.Get()
	defer rc.Close()

	// trigger on our twitter channel for new conversations and favorites flow
	db.MustExec(
		`INSERT INTO triggers_trigger(is_active, created_on, modified_on, keyword, is_archived, 
									  flow_id, trigger_type, match_type, created_by_id, modified_by_id, org_id, trigger_count, channel_id)
		VALUES(TRUE, now(), now(), NULL, false, $1, 'N', NULL, 1, 1, 1, 0, $2) RETURNING id`,
		int(favoritesFlow), twitterChannel)

	// trigger on our nexmo channel for referral and number flow
	db.MustExec(
		`INSERT INTO triggers_trigger(is_active, created_on, modified_on, keyword, is_archived, 
									  flow_id, trigger_type, match_type, created_by_id, modified_by_id, org_id, trigger_count, channel_id)
		VALUES(TRUE, now(), now(), NULL, false, $1, 'R', NULL, 1, 1, 1, 0, $2) RETURNING id`,
		int(numberFlow), nexmoChannel)

	// add a URN for cathy so we can test twitter URNs
	var cathyTwitterURN models.URNID
	db.Get(&cathyTwitterURN,
		`INSERT INTO contacts_contacturn(identity, path, scheme, priority, contact_id, org_id) 
		                          VALUES('twitterid:123456', '123456', 'twitterid', 10, $1, 1) RETURNING id`,
		cathy)

	tcs := []struct {
		EventType string
		ContactID flows.ContactID
		URNID     models.URNID
		OrgID     models.OrgID
		ChannelID models.ChannelID
		Extra     map[string]string
		Response  string
	}{
		{newConversationEventType, cathy, cathyTwitterURN, org1, twitterChannel, nil, "What is your favorite color?"},
		{newConversationEventType, cathy, cathyURNID, org1, nexmoChannel, nil, ""},
		{referralEventType, cathy, cathyTwitterURN, org1, twitterChannel, nil, ""},
		{referralEventType, cathy, cathyURNID, org1, nexmoChannel, nil, "Pick a number between 1-10."},
	}

	last := time.Now()
	for i, tc := range tcs {
		time.Sleep(10 * time.Millisecond)

		event := &channelEvent{
			ContactID: tc.ContactID,
			URNID:     tc.URNID,
			OrgID:     tc.OrgID,
			ChannelID: tc.ChannelID,
			Extra:     tc.Extra,
		}

		eventJSON, err := json.Marshal(event)
		assert.NoError(t, err)

		task := &queue.Task{
			Type:  tc.EventType,
			OrgID: int(tc.OrgID),
			Task:  eventJSON,
		}

		err = AddHandleTask(rc, tc.ContactID, task)
		assert.NoError(t, err, "%d: error adding task", i)

		task, err = queue.PopNextTask(rc, mailroom.HandlerQueue)
		assert.NoError(t, err, "%d: error popping next task", i)

		err = handleContactEvent(ctx, db, rp, task)
		assert.NoError(t, err, "%d: error when handling event", i)

		// if we are meant to have a response
		var text string
		db.Get(&text, `SELECT text FROM msgs_msg WHERE contact_id = $1 AND contact_urn_id = $2 AND created_on > $3 ORDER BY id DESC LIMIT 1`, tc.ContactID, tc.URNID, last)
		assert.Equal(t, text, tc.Response, "%d: response: '%s' is not '%s'", i, text, tc.Response)
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
		favoritesFlow,
	)

	tcs := []struct {
		EventType string
		ContactID flows.ContactID
		URN       urns.URN
		URNID     models.URNID
		Message   string
		Response  string
		ChannelID models.ChannelID
		OrgID     models.OrgID
	}{
		// start the flow
		{msgEventType, cathy, cathyURN, cathyURNID, "start", "What is your favorite color?", twitterChannel, org1},

		// this expiration does nothing because the times don't match
		{expirationEventType, cathy, cathyURN, cathyURNID, "bad", "", twitterChannel, org1},

		// this checks that the flow wasn't expired
		{msgEventType, cathy, cathyURN, cathyURNID, "red", "Good choice, I like Red too! What is your favorite beer?", twitterChannel, org1},

		// this expiration will actually take
		{expirationEventType, cathy, cathyURN, cathyURNID, "good", "", twitterChannel, org1},

		// we won't get a response as we will be out of the flow
		{msgEventType, cathy, cathyURN, cathyURNID, "mutzig", "", twitterChannel, org1},
	}

	last := time.Now()
	var sessionID models.SessionID
	var runID models.FlowRunID
	var runExpiration time.Time

	for i, tc := range tcs {
		time.Sleep(10 * time.Millisecond)

		var task *queue.Task
		if tc.EventType == msgEventType {
			event := &msgEvent{
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
		} else if tc.EventType == expirationEventType {
			var expiration time.Time
			if tc.Message == "bad" {
				expiration = time.Now()
			} else {
				expiration = runExpiration
			}

			task = newTimedTask(
				expirationEventType,
				tc.OrgID,
				tc.ContactID,
				sessionID,
				runID,
				expiration,
			)
		}

		err := AddHandleTask(rc, tc.ContactID, task)
		assert.NoError(t, err, "%d: error adding task", i)

		task, err = queue.PopNextTask(rc, mailroom.HandlerQueue)
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
