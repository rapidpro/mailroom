package handler

import (
	"encoding/json"
	"strings"
	"testing"

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
	org1       = models.OrgID(1)
	cathy      = flows.ContactID(42)
	cathyURN   = urns.URN("tel:+250700000001")
	cathyURNID = models.URNID(42)
	channel1   = models.ChannelID(3)

	org2        = models.OrgID(2)
	george      = flows.ContactID(54)
	georgeURN   = urns.URN("tel:+250700000013")
	georgeURNID = models.URNID(55)
	channel2    = models.ChannelID(5)
)

func TestHandler(t *testing.T) {
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
		{cathy, cathyURN, cathyURNID, "noop", "", channel1, org1},
		{cathy, cathyURN, cathyURNID, "start other", "", channel1, org1},
		{cathy, cathyURN, cathyURNID, "start", "What is your favorite color?", channel1, org1},
		{cathy, cathyURN, cathyURNID, "purple", "I don't know that color. Try again.", channel1, org1},
		{cathy, cathyURN, cathyURNID, "blue", "Good choice, I like Blue too! What is your favorite beer?", channel1, org1},
		{cathy, cathyURN, cathyURNID, "MUTZIG", "Mmmmm... delicious Mutzig. If only they made blue Mutzig! Lastly, what is your name?", channel1, org1},
		{cathy, cathyURN, cathyURNID, "Cathy", "Thanks Cathy, we are all done!", channel1, org1},
		{cathy, cathyURN, cathyURNID, "noop", "Thanks Cathy, we are all done!", channel1, org1},

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
