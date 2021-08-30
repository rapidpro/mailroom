package models_test

import (
	"testing"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/require"
)

func TestNotifications(t *testing.T) {
	ctx, _, db, _ := testsuite.Reset()

	oa, err := models.GetOrgAssets(ctx, db, testdata.Org1.ID)
	require.NoError(t, err)

	ticket := testdata.InsertOpenTicket(db, testdata.Org1, testdata.Cathy, testdata.Internal, testdata.SupportTopic, "", "Where my pants", "", nil)
	modelTicket := ticket.Load(db)

	err = models.LogTicketOpened(ctx, db, oa, modelTicket)
	require.NoError(t, err)

	testsuite.AssertQuery(t, db, `SELECT org_id, log_type, ticket_id FROM notifications_log`).
		Columns(map[string]interface{}{"org_id": int64(testdata.Org1.ID), "log_type": "ticket:opened", "ticket_id": int64(modelTicket.ID())})

	testsuite.AssertQuery(t, db, `SELECT count(*) FROM notifications_notification`).Returns(3)
}
