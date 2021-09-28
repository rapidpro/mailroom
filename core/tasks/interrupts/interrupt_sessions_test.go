package interrupts

import (
	"testing"

	"github.com/nyaruka/gocommon/uuids"
	_ "github.com/nyaruka/mailroom/core/handlers"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"

	"github.com/stretchr/testify/assert"
)

func TestInterrupts(t *testing.T) {
	ctx, rt, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetAll)

	insertConnection := func(orgID models.OrgID, channelID models.ChannelID, contactID models.ContactID, urnID models.URNID) models.ConnectionID {
		var connectionID models.ConnectionID
		err := db.Get(&connectionID,
			`INSERT INTO channels_channelconnection(created_on, modified_on, external_id, status, direction, connection_type, error_count, org_id, channel_id, contact_id, contact_urn_id) 
 						VALUES(NOW(), NOW(), 'ext1', 'I', 'I', 'V', 0, $1, $2, $3, $4) RETURNING id`,
			orgID, channelID, contactID, urnID,
		)
		assert.NoError(t, err)
		return connectionID
	}

	insertSession := func(orgID models.OrgID, contactID models.ContactID, connectionID models.ConnectionID, currentFlowID models.FlowID) models.SessionID {
		var sessionID models.SessionID
		err := db.Get(&sessionID,
			`INSERT INTO flows_flowsession(uuid, status, responded, created_on, org_id, contact_id, connection_id, current_flow_id, session_type)
									VALUES($1, 'W', false, NOW(), $2, $3, $4, $5, 'M') RETURNING id`,
			uuids.New(), orgID, contactID, connectionID, currentFlowID)
		assert.NoError(t, err)

		// give session one active run too
		db.MustExec(`INSERT INTO flows_flowrun(uuid, is_active, status, created_on, modified_on, responded, contact_id, flow_id, session_id, org_id)
			                            VALUES($1, TRUE, 'W', now(), now(), FALSE, $2, $3, $4, 1);`, uuids.New(), contactID, currentFlowID, sessionID)

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
		twilioConnectionID := insertConnection(testdata.Org1.ID, testdata.TwilioChannel.ID, testdata.Alexandria.ID, testdata.Alexandria.URNID)

		sessionIDs := make([]models.SessionID, 5)

		// insert our dummy contact sessions
		sessionIDs[0] = insertSession(testdata.Org1.ID, testdata.Cathy.ID, models.NilConnectionID, testdata.Favorites.ID)
		sessionIDs[1] = insertSession(testdata.Org1.ID, testdata.George.ID, models.NilConnectionID, testdata.Favorites.ID)
		sessionIDs[2] = insertSession(testdata.Org1.ID, testdata.Alexandria.ID, twilioConnectionID, testdata.Favorites.ID)
		sessionIDs[3] = insertSession(testdata.Org1.ID, testdata.Bob.ID, models.NilConnectionID, testdata.PickANumber.ID)

		// a session we always end explicitly
		sessionIDs[4] = insertSession(testdata.Org1.ID, testdata.Bob.ID, models.NilConnectionID, testdata.Favorites.ID)

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
			testsuite.AssertQuery(t, db, `SELECT count(*) FROM flows_flowrun WHERE session_id = $1 AND status != $2`, sID, tc.StatusesAfter[j]).
				Returns(0, "%d: unexpected un-interrupted runs for session #%d", i, j)
		}
	}
}
