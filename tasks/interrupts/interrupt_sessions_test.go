package interrupts

import (
	"testing"

	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/config"
	_ "github.com/nyaruka/mailroom/hooks"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/testsuite"

	"github.com/stretchr/testify/assert"
)

func TestInterrupts(t *testing.T) {
	testsuite.Reset()
	ctx := testsuite.CTX()
	db := testsuite.DB()

	mr := &mailroom.Mailroom{Config: config.Mailroom, DB: db, RP: testsuite.RP(), ElasticClient: nil}

	insertConnection := func(orgID models.OrgID, channelID models.ChannelID, contactID models.ContactID, urnID models.URNID) models.ConnectionID {
		var connectionID models.ConnectionID
		err := db.Get(&connectionID,
			`INSERT INTO channels_channelconnection(created_on, modified_on, external_id, status, direction, connection_type, retry_count, error_count, org_id, channel_id, contact_id, contact_urn_id) 
 						VALUES(NOW(), NOW(), 'ext1', 'I', 'I', 'V', 0, 0, $1, $2, $3, $4) RETURNING id`,
			orgID, channelID, contactID, urnID,
		)
		assert.NoError(t, err)
		return connectionID
	}

	insertSession := func(orgID models.OrgID, contactID models.ContactID, connectionID models.ConnectionID, currentFlowID models.FlowID) models.SessionID {
		var sessionID models.SessionID
		err := db.Get(&sessionID,
			`INSERT INTO flows_flowsession(uuid, status, responded, created_on, org_id, contact_id, connection_id, current_flow_id)
									VALUES($1, 'W', false, NOW(), $2, $3, $4, $5) RETURNING id`,
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
			[]models.ContactID{models.CathyID}, nil, nil,
			[5]string{"I", "W", "W", "W", "I"},
		},
		{
			[]models.ContactID{models.CathyID, models.GeorgeID}, nil, nil,
			[5]string{"I", "I", "W", "W", "I"},
		},
		{
			nil, []models.ChannelID{models.TwilioChannelID}, nil,
			[5]string{"W", "W", "I", "W", "I"},
		},
		{
			nil, nil, []models.FlowID{models.PickNumberFlowID},
			[5]string{"W", "W", "W", "I", "I"},
		},
		{
			[]models.ContactID{models.CathyID, models.GeorgeID}, []models.ChannelID{models.TwilioChannelID}, []models.FlowID{models.PickNumberFlowID},
			[5]string{"I", "I", "I", "I", "I"},
		},
	}

	for i, tc := range tcs {
		// mark any remaining flow sessions as inactive
		db.MustExec(`UPDATE flows_flowsession SET status='C', ended_on=NOW() WHERE status = 'W';`)

		// twilio connection
		twilioConnectionID := insertConnection(models.Org1, models.TwilioChannelID, models.AlexandriaID, models.AlexandriaURNID)

		sessionIDs := make([]models.SessionID, 5)

		// insert our dummy contact sessions
		sessionIDs[0] = insertSession(models.Org1, models.CathyID, models.NilConnectionID, models.FavoritesFlowID)
		sessionIDs[1] = insertSession(models.Org1, models.GeorgeID, models.NilConnectionID, models.FavoritesFlowID)
		sessionIDs[2] = insertSession(models.Org1, models.AlexandriaID, twilioConnectionID, models.FavoritesFlowID)
		sessionIDs[3] = insertSession(models.Org1, models.BobID, models.NilConnectionID, models.PickNumberFlowID)

		// a session we always end explicitly
		sessionIDs[4] = insertSession(models.Org1, models.BobID, models.NilConnectionID, models.FavoritesFlowID)

		// create our task
		task := &InterruptSessionsTask{
			SessionIDs: []models.SessionID{sessionIDs[4]},
			ContactIDs: tc.ContactIDs,
			ChannelIDs: tc.ChannelIDs,
			FlowIDs:    tc.FlowIDs,
		}

		// execute it
		err := task.Perform(ctx, mr)
		assert.NoError(t, err)

		// check session statuses are as expected
		for j, sID := range sessionIDs {
			var status string
			err := db.Get(&status, `SELECT status FROM flows_flowsession WHERE id = $1`, sID)
			assert.NoError(t, err)
			assert.Equal(t, tc.StatusesAfter[j], status, "%d: status mismatch for session #%d", i, j)

			// check for runs with a different status to the session
			testsuite.AssertQueryCount(
				t, db,
				`SELECT count(*) FROM flows_flowrun WHERE session_id = $1 AND status != $2`,
				[]interface{}{sID, tc.StatusesAfter[j]}, 0,
				"%d: unexpected un-interrupted runs for session #%d", i, j,
			)
		}
	}
}
