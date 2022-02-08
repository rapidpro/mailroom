package models_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/test"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStarts(t *testing.T) {
	ctx, _, db, _ := testsuite.Get()

	startID := testdata.InsertFlowStart(db, testdata.Org1, testdata.SingleMessage, []*testdata.Contact{testdata.Cathy, testdata.Bob})

	startJSON := []byte(fmt.Sprintf(`{
		"start_id": %d,
		"start_type": "M",
		"org_id": %d,
		"created_by_id": %d,
		"flow_id": %d,
		"flow_type": "M",
		"contact_ids": [%d, %d],
		"group_ids": [%d],
		"exclude_group_ids": [%d],
		"urns": ["tel:+12025550199"],
		"query": null,
		"restart_participants": true,
		"include_active": true,
		"parent_summary": {"uuid": "b65b1a22-db6d-4f5a-9b3d-7302368a82e6"},
		"session_history": {"parent_uuid": "532a3899-492f-4ffe-aed7-e75ad524efab", "ancestors": 3, "ancestors_since_input": 1},
		"extra": {"foo": "bar"}
	}`, startID, testdata.Org1.ID, testdata.Admin.ID, testdata.SingleMessage.ID, testdata.Cathy.ID, testdata.Bob.ID, testdata.DoctorsGroup.ID, testdata.TestersGroup.ID))

	start := &models.FlowStart{}
	err := json.Unmarshal(startJSON, start)

	require.NoError(t, err)
	assert.Equal(t, startID, start.ID())
	assert.Equal(t, testdata.Org1.ID, start.OrgID())
	assert.Equal(t, testdata.Admin.ID, start.CreatedByID())
	assert.Equal(t, testdata.SingleMessage.ID, start.FlowID())
	assert.Equal(t, models.FlowTypeMessaging, start.FlowType())
	assert.Equal(t, "", start.Query())
	assert.True(t, start.RestartParticipants())
	assert.True(t, start.IncludeActive())
	assert.Equal(t, []models.ContactID{testdata.Cathy.ID, testdata.Bob.ID}, start.ContactIDs())
	assert.Equal(t, []models.GroupID{testdata.DoctorsGroup.ID}, start.GroupIDs())
	assert.Equal(t, []models.GroupID{testdata.TestersGroup.ID}, start.ExcludeGroupIDs())

	assert.Equal(t, json.RawMessage(`{"uuid": "b65b1a22-db6d-4f5a-9b3d-7302368a82e6"}`), start.ParentSummary())
	assert.Equal(t, json.RawMessage(`{"parent_uuid": "532a3899-492f-4ffe-aed7-e75ad524efab", "ancestors": 3, "ancestors_since_input": 1}`), start.SessionHistory())
	assert.Equal(t, json.RawMessage(`{"foo": "bar"}`), start.Extra())

	err = models.MarkStartStarted(ctx, db, startID, 2, []models.ContactID{testdata.George.ID})
	require.NoError(t, err)

	assertdb.Query(t, db, `SELECT count(*) FROM flows_flowstart WHERE id = $1 AND status = 'S' AND contact_count = 2`, startID).Returns(1)
	assertdb.Query(t, db, `SELECT count(*) FROM flows_flowstart_contacts WHERE flowstart_id = $1`, startID).Returns(3)

	batch := start.CreateBatch([]models.ContactID{testdata.Cathy.ID, testdata.Bob.ID}, false, 3)
	assert.Equal(t, startID, batch.StartID())
	assert.Equal(t, models.StartTypeManual, batch.StartType())
	assert.Equal(t, testdata.SingleMessage.ID, batch.FlowID())
	assert.Equal(t, []models.ContactID{testdata.Cathy.ID, testdata.Bob.ID}, batch.ContactIDs())
	assert.True(t, batch.RestartParticipants())
	assert.True(t, batch.IncludeActive())
	assert.Equal(t, testdata.Admin.ID, batch.CreatedByID())
	assert.False(t, batch.IsLast())
	assert.Equal(t, 3, batch.TotalContacts())

	assert.Equal(t, json.RawMessage(`{"uuid": "b65b1a22-db6d-4f5a-9b3d-7302368a82e6"}`), batch.ParentSummary())
	assert.Equal(t, json.RawMessage(`{"parent_uuid": "532a3899-492f-4ffe-aed7-e75ad524efab", "ancestors": 3, "ancestors_since_input": 1}`), batch.SessionHistory())
	assert.Equal(t, json.RawMessage(`{"foo": "bar"}`), batch.Extra())

	history, err := models.ReadSessionHistory(batch.SessionHistory())
	assert.NoError(t, err)
	assert.Equal(t, flows.SessionUUID("532a3899-492f-4ffe-aed7-e75ad524efab"), history.ParentUUID)

	_, err = models.ReadSessionHistory([]byte(`{`))
	assert.EqualError(t, err, "unexpected end of JSON input")

	err = models.MarkStartComplete(ctx, db, startID)
	require.NoError(t, err)

	assertdb.Query(t, db, `SELECT count(*) FROM flows_flowstart WHERE id = $1 AND status = 'C'`, startID).Returns(1)
}

func TestStartsBuilding(t *testing.T) {
	uuids.SetGenerator(uuids.NewSeededGenerator(12345))
	defer uuids.SetGenerator(uuids.DefaultGenerator)

	start := models.NewFlowStart(testdata.Org1.ID, models.StartTypeManual, models.FlowTypeMessaging, testdata.Favorites.ID, true, true).
		WithGroupIDs([]models.GroupID{testdata.DoctorsGroup.ID}).
		WithExcludeGroupIDs([]models.GroupID{testdata.TestersGroup.ID}).
		WithContactIDs([]models.ContactID{testdata.Cathy.ID, testdata.Bob.ID}).
		WithQuery(`language != ""`).
		WithCreateContact(true)

	marshalled, err := jsonx.Marshal(start)
	require.NoError(t, err)

	test.AssertEqualJSON(t, []byte(fmt.Sprintf(`{
		"UUID": "1ae96956-4b34-433e-8d1a-f05fe6923d6d",
		"contact_ids": [%d, %d],
		"create_contact": true,
		"created_by_id": null,
		"exclude_group_ids": [%d],
		"flow_id": %d,
		"flow_type": "M",
		"group_ids": [%d],
		"include_active": true,
		"org_id": 1,
		"query": "language != \"\"",
		"restart_participants": true,
		"start_id": null,
		"start_type": "M"
	}`, testdata.Cathy.ID, testdata.Bob.ID, testdata.TestersGroup.ID, testdata.Favorites.ID, testdata.DoctorsGroup.ID)), marshalled)
}
