package models_test

import (
	"testing"
	"time"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/mailroom/core/goflow"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/assert"
)

func TestLoadFlows(t *testing.T) {
	ctx, rt, db, _ := testsuite.Get()

	db.MustExec(`UPDATE flows_flow SET metadata = '{"ivr_retry": 30}'::json WHERE id = $1`, testdata.IVRFlow.ID)
	db.MustExec(`UPDATE flows_flow SET metadata = '{"ivr_retry": -1}'::json WHERE id = $1`, testdata.SurveyorFlow.ID)

	sixtyMinutes := 60 * time.Minute
	thirtyMinutes := 30 * time.Minute

	tcs := []struct {
		org              *testdata.Org
		flowID           models.FlowID
		flowUUID         assets.FlowUUID
		expectedName     string
		expectedIVRRetry *time.Duration
	}{
		{testdata.Org1, testdata.Favorites.ID, testdata.Favorites.UUID, "Favorites", &sixtyMinutes},    // will use default IVR retry
		{testdata.Org1, testdata.IVRFlow.ID, testdata.IVRFlow.UUID, "IVR Flow", &thirtyMinutes},        // will have explicit IVR retry
		{testdata.Org1, testdata.SurveyorFlow.ID, testdata.SurveyorFlow.UUID, "Contact Surveyor", nil}, // will have no IVR retry
		{testdata.Org2, models.FlowID(0), assets.FlowUUID("51e3c67d-8483-449c-abf7-25e50686f0db"), "", nil},
	}

	for i, tc := range tcs {
		// test loading by UUID
		flow, err := models.LoadFlowByUUID(ctx, db, tc.org.ID, tc.flowUUID)
		assert.NoError(t, err)

		if tc.expectedName != "" {
			assert.Equal(t, tc.flowID, flow.ID())
			assert.Equal(t, tc.flowUUID, flow.UUID())
			assert.Equal(t, tc.expectedName, flow.Name(), "%d: name mismatch", i)
			assert.Equal(t, tc.expectedIVRRetry, flow.IVRRetryWait(), "%d: IVR retry mismatch", i)

			_, err := goflow.ReadFlow(rt.Config, flow.Definition())
			assert.NoError(t, err)
		} else {
			assert.Nil(t, flow)
		}

		// test loading by ID
		flow, err = models.LoadFlowByID(ctx, db, tc.org.ID, tc.flowID)
		assert.NoError(t, err)

		if tc.expectedName != "" {
			assert.Equal(t, tc.flowID, flow.ID())
			assert.Equal(t, tc.flowUUID, flow.UUID())
			assert.Equal(t, tc.expectedName, flow.Name(), "%d: name mismatch", i)
			assert.Equal(t, tc.expectedIVRRetry, flow.IVRRetryWait(), "%d: IVR retry mismatch", i)
		} else {
			assert.Nil(t, flow)
		}
	}
}

func TestFlowIDForUUID(t *testing.T) {
	ctx, rt, db, _ := testsuite.Get()

	org, _ := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)

	tx, err := db.BeginTxx(ctx, nil)
	assert.NoError(t, err)

	id, err := models.FlowIDForUUID(ctx, tx, org, testdata.Favorites.UUID)
	assert.NoError(t, err)
	assert.Equal(t, testdata.Favorites.ID, id)

	// make favorite inactive
	tx.MustExec(`UPDATE flows_flow SET is_active = FALSE WHERE id = $1`, testdata.Favorites.ID)
	tx.Commit()

	tx, err = db.BeginTxx(ctx, nil)
	assert.NoError(t, err)
	defer tx.Rollback()

	// clear our assets so it isn't cached
	models.FlushCache()
	org, _ = models.GetOrgAssets(ctx, rt, testdata.Org1.ID)

	id, err = models.FlowIDForUUID(ctx, tx, org, testdata.Favorites.UUID)
	assert.NoError(t, err)
	assert.Equal(t, testdata.Favorites.ID, id)
}
