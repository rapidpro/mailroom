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

func TestFlows(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()
	rt := testsuite.RT()

	db.MustExec(`UPDATE flows_flow SET metadata = '{"ivr_retry": 30}'::json WHERE id = $1`, testdata.IVRFlow.ID)

	tcs := []struct {
		OrgID    models.OrgID
		FlowID   models.FlowID
		FlowUUID assets.FlowUUID
		Name     string
		IVRRetry time.Duration
		Found    bool
	}{
		{testdata.Org1.ID, testdata.Favorites.ID, testdata.Favorites.UUID, "Favorites", 60 * time.Minute, true},
		{testdata.Org1.ID, testdata.IVRFlow.ID, testdata.IVRFlow.UUID, "IVR Flow", 30 * time.Minute, true},
		{testdata.Org2.ID, models.FlowID(0), assets.FlowUUID("51e3c67d-8483-449c-abf7-25e50686f0db"), "", 0, false},
	}

	for _, tc := range tcs {
		flow, err := models.LoadFlowByUUID(ctx, db, tc.OrgID, tc.FlowUUID)
		assert.NoError(t, err)

		if tc.Found {
			assert.Equal(t, tc.Name, flow.Name())
			assert.Equal(t, tc.FlowID, flow.ID())
			assert.Equal(t, tc.FlowUUID, flow.UUID())
			assert.Equal(t, tc.IVRRetry, flow.IVRRetryWait())

			_, err := goflow.ReadFlow(rt.Config, flow.Definition())
			assert.NoError(t, err)
		} else {
			assert.Nil(t, flow)
		}

		flow, err = models.LoadFlowByID(ctx, db, tc.OrgID, tc.FlowID)
		assert.NoError(t, err)

		if tc.Found {
			assert.Equal(t, tc.Name, flow.Name())
			assert.Equal(t, tc.FlowID, flow.ID())
			assert.Equal(t, tc.FlowUUID, flow.UUID())
		} else {
			assert.Nil(t, flow)
		}
	}
}

func TestFlowIDForUUID(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()
	org, _ := models.GetOrgAssets(ctx, db, testdata.Org1.ID)

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
	org, _ = models.GetOrgAssets(ctx, db, testdata.Org1.ID)

	id, err = models.FlowIDForUUID(ctx, tx, org, testdata.Favorites.UUID)
	assert.NoError(t, err)
	assert.Equal(t, testdata.Favorites.ID, id)
}
