package testdata

import (
	"testing"

	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/mailroom/models"

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
