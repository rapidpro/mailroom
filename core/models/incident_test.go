package models_test

import (
	"testing"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIncidentUniqueness(t *testing.T) {
	ctx, rt, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetData)

	oa, err := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
	require.NoError(t, err)

	id1, err := models.IncidentWebhooksUnhealthy(ctx, db, oa)
	require.NoError(t, err)
	assert.NotEqual(t, 0, id1)

	testsuite.AssertQuery(t, db, `SELECT count(*) FROM notifications_incident`).Returns(1)

	// raising same incident doesn't create a new one...
	id2, err := models.IncidentWebhooksUnhealthy(ctx, db, oa)
	require.NoError(t, err)
	assert.Equal(t, id1, id2)

	testsuite.AssertQuery(t, db, `SELECT count(*) FROM notifications_incident`).Returns(1)

	// until the first one has ended
	db.MustExec(`UPDATE notifications_incident SET ended_on = NOW()`)

	id3, err := models.IncidentWebhooksUnhealthy(ctx, db, oa)
	require.NoError(t, err)
	assert.NotEqual(t, id1, id3)

	testsuite.AssertQuery(t, db, `SELECT count(*) FROM notifications_incident`).Returns(2)
}
