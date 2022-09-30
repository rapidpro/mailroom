package interrupts

import (
	"testing"
	"time"

	"github.com/nyaruka/gocommon/dbutil/assertdb"
	_ "github.com/nyaruka/mailroom/core/handlers"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"

	"github.com/stretchr/testify/assert"
)

func TestInterrupts(t *testing.T) {
	ctx, rt, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetData)

	insertSession := func(org *testdata.Org, contact *testdata.Contact, flow *testdata.Flow, connectionID models.ConnectionID) models.SessionID {
		sessionID := testdata.InsertWaitingSession(db, org, contact, models.FlowTypeMessaging, flow, connectionID, time.Now(), time.Now(), false, nil)

		// give session one waiting run too
		testdata.InsertFlowRun(db, org, sessionID, contact, flow, models.RunStatusWaiting)
		return sessionID
	}

	tcs := []struct {
		ContactIDs    []models.ContactID
		ChannelIDs    []models.ChannelID
		FlowIDs       []models.FlowID
		StatusesAfter [5]string
	}{
		{
			nil, nil, nil,
			[5]string{"W", "W", "W", "W", "I"},
		},
		{
			[]models.ContactID{testdata.Cathy.ID}, nil, nil,
			[5]string{"I", "W", "W", "W", "I"},
		},
		{
			[]models.ContactID{testdata.Cathy.ID, testdata.George.ID}, nil, nil,
			[5]string{"I", "I", "W", "W", "I"},
		},
		{
			nil, []models.ChannelID{testdata.TwilioChannel.ID}, nil,
			[5]string{"W", "W", "I", "W", "I"},
		},
		{
			nil, nil, []models.FlowID{testdata.PickANumber.ID},
			[5]string{"W", "W", "W", "I", "I"},
		},
		{
			[]models.ContactID{testdata.Cathy.ID, testdata.George.ID}, []models.ChannelID{testdata.TwilioChannel.ID}, []models.FlowID{testdata.PickANumber.ID},
			[5]string{"I", "I", "I", "I", "I"},
		},
	}

	for i, tc := range tcs {
		// mark any remaining flow sessions as inactive
		db.MustExec(`UPDATE flows_flowsession SET status='C', ended_on=NOW() WHERE status = 'W';`)

		// twilio connection
		twilioConnectionID := testdata.InsertConnection(db, testdata.Org1, testdata.TwilioChannel, testdata.Alexandria)

		sessionIDs := make([]models.SessionID, 5)

		// insert our dummy contact sessions
		sessionIDs[0] = insertSession(testdata.Org1, testdata.Cathy, testdata.Favorites, models.NilConnectionID)
		sessionIDs[1] = insertSession(testdata.Org1, testdata.George, testdata.Favorites, models.NilConnectionID)
		sessionIDs[2] = insertSession(testdata.Org1, testdata.Alexandria, testdata.Favorites, twilioConnectionID)
		sessionIDs[3] = insertSession(testdata.Org1, testdata.Bob, testdata.PickANumber, models.NilConnectionID)

		// a session we always end explicitly
		sessionIDs[4] = insertSession(testdata.Org1, testdata.Bob, testdata.Favorites, models.NilConnectionID)

		// create our task
		task := &InterruptSessionsTask{
			SessionIDs: []models.SessionID{sessionIDs[4]},
			ContactIDs: tc.ContactIDs,
			ChannelIDs: tc.ChannelIDs,
			FlowIDs:    tc.FlowIDs,
		}

		// execute it
		err := task.Perform(ctx, rt, testdata.Org1.ID)
		assert.NoError(t, err)

		// check session statuses are as expected
		for j, sID := range sessionIDs {
			var status string
			err := db.Get(&status, `SELECT status FROM flows_flowsession WHERE id = $1`, sID)
			assert.NoError(t, err)
			assert.Equal(t, tc.StatusesAfter[j], status, "%d: status mismatch for session #%d", i, j)

			// check for runs with a different status to the session
			assertdb.Query(t, db, `SELECT count(*) FROM flows_flowrun WHERE session_id = $1 AND status != $2`, sID, tc.StatusesAfter[j]).
				Returns(0, "%d: unexpected un-interrupted runs for session #%d", i, j)
		}
	}
}
