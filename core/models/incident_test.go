package models_test

import (
	"testing"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/require"
)

func TestIncidentUniqueness(t *testing.T) {
	ctx, rt, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetData)

	oa, err := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
	require.NoError(t, err)

	err = models.IncidentWebhooksUnhealthy(ctx, db, oa)
	require.NoError(t, err)

	testsuite.AssertQuery(t, db, `SELECT count(*) FROM notifications_incident`).Returns(1)

	// raising same incident doesn't create a new one...
	err = models.IncidentWebhooksUnhealthy(ctx, db, oa)
	require.NoError(t, err)

	testsuite.AssertQuery(t, db, `SELECT count(*) FROM notifications_incident`).Returns(1)

	// until the first one has ended
	db.MustExec(`UPDATE notifications_incident SET ended_on = NOW()`)

	err = models.IncidentWebhooksUnhealthy(ctx, db, oa)
	require.NoError(t, err)

	testsuite.AssertQuery(t, db, `SELECT count(*) FROM notifications_incident`).Returns(2)
}
