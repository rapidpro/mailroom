package interrupts

import (
	"testing"

	_ "github.com/nyaruka/mailroom/hooks"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/stretchr/testify/assert"
)

func TestInterrupts(t *testing.T) {
	testsuite.Reset()
	ctx := testsuite.CTX()
	db := testsuite.DB()

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
			`INSERT INTO flows_flowsession(status, responded, created_on, org_id, contact_id, connection_id, current_flow_id)
									VALUES('W', false, NOW(), $1, $2, $3, $4) RETURNING id`,
			orgID, contactID, connectionID, currentFlowID)
		assert.NoError(t, err)
		return sessionID
	}

	tcs := []struct {
		ContactIDs      []models.ContactID
		ChannelIDs      []models.ChannelID
		FlowIDs         []models.FlowID
		ActiveRemaining int
	}{
		{nil, nil, nil, 4},
		{[]models.ContactID{models.CathyID}, nil, nil, 3},
		{[]models.ContactID{models.CathyID, models.GeorgeID}, nil, nil, 2},
		{nil, []models.ChannelID{models.TwilioChannelID}, nil, 3},
		{nil, nil, []models.FlowID{models.PickNumberFlowID}, 3},
		{[]models.ContactID{models.CathyID, models.GeorgeID}, []models.ChannelID{models.TwilioChannelID}, []models.FlowID{models.PickNumberFlowID}, 0},
	}

	for i, tc := range tcs {
		// mark any remaining flow sessions as inactive
		db.MustExec(`UPDATE flows_flowsession SET status='C', ended_on=NOW() WHERE status = 'W';`)

		// twilio connection
		twilioConnectionID := insertConnection(models.Org1, models.TwilioChannelID, models.AlexandriaID, models.AlexandriaURNID)

		// insert our dummy contact sessions
		insertSession(models.Org1, models.CathyID, models.NilConnectionID, models.FavoritesFlowID)
		insertSession(models.Org1, models.GeorgeID, models.NilConnectionID, models.FavoritesFlowID)
		insertSession(models.Org1, models.AlexandriaID, twilioConnectionID, models.FavoritesFlowID)
		insertSession(models.Org1, models.BobID, models.NilConnectionID, models.PickNumberFlowID)

		// our static session we always end
		sessionID := insertSession(models.Org1, models.BobID, models.NilConnectionID, models.FavoritesFlowID)

		// create our task
		task := &InterruptSessionsTask{
			ContactIDs: tc.ContactIDs,
			ChannelIDs: tc.ChannelIDs,
			SessionIDs: []models.SessionID{sessionID},
			FlowIDs:    tc.FlowIDs,
		}

		// execute it
		err := interruptSessions(ctx, db, task)
		assert.NoError(t, err)

		// check final count of active
		testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM flows_flowsession WHERE status = 'W'`,
			nil, tc.ActiveRemaining, "%d: unexpected active session count", i)
	}
}
