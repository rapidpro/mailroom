package models

import (
	"testing"
	"time"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/mailroom/core/goflow"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/stretchr/testify/assert"
)

func TestFlows(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()

	db.MustExec(`UPDATE flows_flow SET metadata = '{"ivr_retry": 30}'::json WHERE id = $1`, IVRFlowID)

	tcs := []struct {
		OrgID    OrgID
		FlowID   FlowID
		FlowUUID assets.FlowUUID
		Name     string
		IVRRetry time.Duration
		Found    bool
	}{
		{Org1, FavoritesFlowID, FavoritesFlowUUID, "Favorites", 60 * time.Minute, true},
		{Org1, IVRFlowID, IVRFlowUUID, "IVR Flow", 30 * time.Minute, true},
		{Org2, FlowID(0), assets.FlowUUID("51e3c67d-8483-449c-abf7-25e50686f0db"), "", 0, false},
	}

	for _, tc := range tcs {
		flow, err := loadFlowByUUID(ctx, db, tc.OrgID, tc.FlowUUID)
		assert.NoError(t, err)

		if tc.Found {
			assert.Equal(t, tc.Name, flow.Name())
			assert.Equal(t, tc.FlowID, flow.ID())
			assert.Equal(t, tc.FlowUUID, flow.UUID())
			assert.Equal(t, tc.IVRRetry, flow.IVRRetryWait())

			_, err := goflow.ReadFlow(flow.Definition())
			assert.NoError(t, err)
		} else {
			assert.Nil(t, flow)
		}

		flow, err = loadFlowByID(ctx, db, tc.OrgID, tc.FlowID)
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

func TestGetFlowUUID(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()
	org, _ := GetOrgAssets(ctx, db, Org1)

	tx, err := db.BeginTxx(ctx, nil)
	assert.NoError(t, err)

	id, err := flowIDForUUID(ctx, tx, org, FavoritesFlowUUID)
	assert.NoError(t, err)
	assert.Equal(t, FavoritesFlowID, id)

	// make favorite inactive
	tx.MustExec(`UPDATE flows_flow SET is_active = FALSE WHERE id = $1`, FavoritesFlowID)
	tx.Commit()

	tx, err = db.BeginTxx(ctx, nil)
	assert.NoError(t, err)
	defer tx.Rollback()

	// clear our assets so it isn't cached
	FlushCache()
	org, _ = GetOrgAssets(ctx, db, Org1)

	id, err = flowIDForUUID(ctx, tx, org, FavoritesFlowUUID)
	assert.NoError(t, err)
	assert.Equal(t, FavoritesFlowID, id)
}
