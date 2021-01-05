package testdata

import (
	"testing"
	"time"

	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/null"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

// InsertFlowStart inserts a flow start
func InsertFlowStart(t *testing.T, db *sqlx.DB, orgID models.OrgID, flowID models.FlowID, contactIDs []models.ContactID) models.StartID {
	var id models.StartID
	err := db.Get(&id,
		`INSERT INTO flows_flowstart(uuid, org_id, flow_id, start_type, created_on, modified_on, restart_participants, include_active, contact_count, status, created_by_id)
		 VALUES($1, $2, $3, 'M', NOW(), NOW(), TRUE, TRUE, 2, 'P', 1) RETURNING id`, uuids.New(), orgID, flowID,
	)
	require.NoError(t, err)

	for i := range contactIDs {
		db.MustExec(`INSERT INTO flows_flowstart_contacts(flowstart_id, contact_id) VALUES($1, $2)`, id, contactIDs[i])
	}

	return id
}

// InsertFlowSession inserts a flow session
func InsertFlowSession(t *testing.T, db *sqlx.DB, uuid flows.SessionUUID, orgID models.OrgID, contactID models.ContactID, status models.SessionStatus, timeoutOn *time.Time) models.SessionID {
	var id models.SessionID
	err := db.Get(&id,
		`INSERT INTO flows_flowsession(uuid, org_id, contact_id, status, responded, created_on, timeout_on) 
		 VALUES($1, $2, $3, $4, TRUE, NOW(), $5) RETURNING id`, uuid, orgID, contactID, status, timeoutOn,
	)
	require.NoError(t, err)
	return id
}

// InsertFlowRun inserts a flow run
func InsertFlowRun(t *testing.T, db *sqlx.DB, uuid flows.RunUUID, orgID models.OrgID, sessionID models.SessionID, contactID models.ContactID, flowID models.FlowID, status models.RunStatus, parent flows.RunUUID, expiresOn *time.Time) models.FlowRunID {
	isActive := status == models.RunStatusActive || status == models.RunStatusWaiting

	var id models.FlowRunID
	err := db.Get(&id,
		`INSERT INTO flows_flowrun(uuid, org_id, session_id, contact_id, flow_id, status, is_active, parent_uuid, responded, created_on, modified_on, expires_on) 
		 VALUES($1, $2, $3, $4, $5, $6, $7, $8, TRUE, NOW(), NOW(), $9) RETURNING id`, uuid, orgID, null.Int(sessionID), contactID, flowID, status, isActive, null.String(parent), expiresOn,
	)
	require.NoError(t, err)
	return id
}
