package models_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/goflow"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/assert"
)

func TestLoadFlows(t *testing.T) {
	ctx, rt, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetAll)

	db.MustExec(`UPDATE flows_flow SET metadata = '{"ivr_retry": 30}'::json WHERE id = $1`, testdata.IVRFlow.ID)
	db.MustExec(`UPDATE flows_flow SET metadata = '{"ivr_retry": -1}'::json WHERE id = $1`, testdata.SurveyorFlow.ID)
	db.MustExec(`UPDATE flows_flow SET expires_after_minutes = 720 WHERE id = $1`, testdata.Favorites.ID)
	db.MustExec(`UPDATE flows_flow SET expires_after_minutes = 1 WHERE id = $1`, testdata.PickANumber.ID)          // too small for messaging
	db.MustExec(`UPDATE flows_flow SET expires_after_minutes = 12345678 WHERE id = $1`, testdata.SingleMessage.ID) // too large for messaging
	db.MustExec(`UPDATE flows_flow SET expires_after_minutes = 123 WHERE id = $1`, testdata.SurveyorFlow.ID)       // surveyor flows shouldn't have expires

	sixtyMinutes := 60 * time.Minute
	thirtyMinutes := 30 * time.Minute

	type testcase struct {
		org                *testdata.Org
		id                 models.FlowID
		uuid               assets.FlowUUID
		name               string
		expectedType       models.FlowType
		expectedEngineType flows.FlowType
		expectedExpire     int
		expectedIVRRetry   *time.Duration
	}

	tcs := []testcase{
		{
			testdata.Org1,
			testdata.Favorites.ID,
			testdata.Favorites.UUID,
			"Favorites",
			models.FlowTypeMessaging,
			flows.FlowTypeMessaging,
			720,
			&sixtyMinutes, // uses default
		},
		{
			testdata.Org1,
			testdata.PickANumber.ID,
			testdata.PickANumber.UUID,
			"Pick a Number",
			models.FlowTypeMessaging,
			flows.FlowTypeMessaging,
			5,             // clamped to minimum
			&sixtyMinutes, // uses default
		},
		{
			testdata.Org1,
			testdata.SingleMessage.ID,
			testdata.SingleMessage.UUID,
			"Send All",
			models.FlowTypeMessaging,
			flows.FlowTypeMessaging,
			43200,         // clamped to maximum
			&sixtyMinutes, // uses default
		},
		{
			testdata.Org1,
			testdata.IVRFlow.ID,
			testdata.IVRFlow.UUID,
			"IVR Flow",
			models.FlowTypeVoice,
			flows.FlowTypeVoice,
			5,
			&thirtyMinutes, // uses explicit
		},
		{
			testdata.Org1,
			testdata.SurveyorFlow.ID,
			testdata.SurveyorFlow.UUID,
			"Contact Surveyor",
			models.FlowTypeSurveyor,
			flows.FlowTypeMessagingOffline,
			0,   // explicit ignored
			nil, // no retry
		},
	}

	assertFlow := func(tc *testcase, dbFlow *models.Flow) {
		desc := fmt.Sprintf("flow id=%d uuid=%s name=%s", tc.id, tc.uuid, tc.name)

		// check properties of flow model
		assert.Equal(t, tc.id, dbFlow.ID())
		assert.Equal(t, tc.uuid, dbFlow.UUID())
		assert.Equal(t, tc.name, dbFlow.Name(), "db name mismatch for %s", desc)
		assert.Equal(t, tc.expectedIVRRetry, dbFlow.IVRRetryWait(), "db IVR retry mismatch for %s", desc)

		// load as engine flow and check that too
		flow, err := goflow.ReadFlow(rt.Config, dbFlow.Definition())
		assert.NoError(t, err, "read flow failed for %s", desc)

		assert.Equal(t, tc.uuid, flow.UUID(), "engine UUID mismatch for %s", desc)
		assert.Equal(t, tc.name, flow.Name(), "engine name mismatch for %s", desc)
		assert.Equal(t, tc.expectedEngineType, flow.Type(), "engine type mismatch for %s", desc)
		assert.Equal(t, tc.expectedExpire, flow.ExpireAfterMinutes(), "engine expire mismatch for %s", desc)

	}

	for _, tc := range tcs {
		// test loading by UUID
		dbFlow, err := models.LoadFlowByUUID(ctx, db, tc.org.ID, tc.uuid)
		assert.NoError(t, err)
		assertFlow(&tc, dbFlow)

		// test loading by name
		dbFlow, err = models.LoadFlowByName(ctx, db, tc.org.ID, tc.name)
		assert.NoError(t, err)
		assertFlow(&tc, dbFlow)

		// test loading by ID
		dbFlow, err = models.LoadFlowByID(ctx, db, tc.org.ID, tc.id)
		assert.NoError(t, err)
		assertFlow(&tc, dbFlow)
	}

	// test loading flow with wrong org
	dbFlow, err := models.LoadFlowByID(ctx, db, testdata.Org2.ID, testdata.Favorites.ID)
	assert.NoError(t, err)
	assert.Nil(t, dbFlow)
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
