package interrupts_test

import (
	"testing"
	"time"

	"github.com/nyaruka/gocommon/dbutil/assertdb"
	_ "github.com/nyaruka/mailroom/core/handlers"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/tasks/interrupts"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/assert"
)

func TestInterrupts(t *testing.T) {
	ctx, rt, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetData)

	insertSession := func(org *testdata.Org, contact *testdata.Contact, flow *testdata.Flow, connectionID models.CallID) models.SessionID {
		sessionID := testdata.InsertWaitingSession(db, org, contact, models.FlowTypeMessaging, flow, connectionID, time.Now(), time.Now(), false, nil)

		// give session one waiting run too
		testdata.InsertFlowRun(db, org, sessionID, contact, flow, models.RunStatusWaiting)
		return sessionID
	}

	tcs := []struct {
		contactIDs       []models.ContactID
		flowIDs          []models.FlowID
		expectedStatuses [5]string
	}{
		{
			contactIDs:       nil,
			flowIDs:          nil,
			expectedStatuses: [5]string{"W", "W", "W", "W", "I"},
		},
		{
			contactIDs:       []models.ContactID{testdata.Cathy.ID},
			flowIDs:          nil,
			expectedStatuses: [5]string{"I", "W", "W", "W", "I"},
		},
		{
			contactIDs:       []models.ContactID{testdata.Cathy.ID, testdata.George.ID},
			flowIDs:          nil,
			expectedStatuses: [5]string{"I", "I", "W", "W", "I"},
		},
		{
			contactIDs:       nil,
			flowIDs:          []models.FlowID{testdata.PickANumber.ID},
			expectedStatuses: [5]string{"W", "W", "W", "I", "I"},
		},
		{
			contactIDs:       []models.ContactID{testdata.Cathy.ID, testdata.George.ID},
			flowIDs:          []models.FlowID{testdata.PickANumber.ID},
			expectedStatuses: [5]string{"I", "I", "W", "I", "I"},
		},
	}

	for i, tc := range tcs {
		// mark any remaining flow sessions as inactive
		db.MustExec(`UPDATE flows_flowsession SET status='C', ended_on=NOW() WHERE status = 'W';`)

		// twilio call
		twilioCallID := testdata.InsertCall(db, testdata.Org1, testdata.TwilioChannel, testdata.Alexandria)

		sessionIDs := make([]models.SessionID, 5)

		// insert our dummy contact sessions
		sessionIDs[0] = insertSession(testdata.Org1, testdata.Cathy, testdata.Favorites, models.NilCallID)
		sessionIDs[1] = insertSession(testdata.Org1, testdata.George, testdata.Favorites, models.NilCallID)
		sessionIDs[2] = insertSession(testdata.Org1, testdata.Alexandria, testdata.Favorites, twilioCallID)
		sessionIDs[3] = insertSession(testdata.Org1, testdata.Bob, testdata.PickANumber, models.NilCallID)

		// a session we always end explicitly
		sessionIDs[4] = insertSession(testdata.Org1, testdata.Bob, testdata.Favorites, models.NilCallID)

		// create our task
		task := &interrupts.InterruptSessionsTask{
			SessionIDs: []models.SessionID{sessionIDs[4]},
			ContactIDs: tc.contactIDs,
			FlowIDs:    tc.flowIDs,
		}

		// execute it
		err := task.Perform(ctx, rt, testdata.Org1.ID)
		assert.NoError(t, err)

		// check session statuses are as expected
		for j, sID := range sessionIDs {
			var status string
			err := db.Get(&status, `SELECT status FROM flows_flowsession WHERE id = $1`, sID)
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedStatuses[j], status, "%d: status mismatch for session #%d", i, j)

			// check for runs with a different status to the session
			assertdb.Query(t, db, `SELECT count(*) FROM flows_flowrun WHERE session_id = $1 AND status != $2`, sID, tc.expectedStatuses[j]).
				Returns(0, "%d: unexpected un-interrupted runs for session #%d", i, j)
		}
	}
}
