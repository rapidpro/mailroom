package models_test

import (
	"testing"

	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/assert"
)

func TestCalls(t *testing.T) {
	ctx, _, db, _ := testsuite.Get()

	defer db.MustExec(`DELETE FROM ivr_call`)

	conn, err := models.InsertCall(ctx, db, testdata.Org1.ID, testdata.TwilioChannel.ID, models.NilStartID, testdata.Cathy.ID, testdata.Cathy.URNID, models.CallDirectionOut, models.CallStatusPending, "")
	assert.NoError(t, err)

	assert.NotEqual(t, models.CallID(0), conn.ID())

	err = conn.UpdateExternalID(ctx, db, "test1")
	assert.NoError(t, err)

	assertdb.Query(t, db, `SELECT count(*) from ivr_call where external_id = 'test1' AND id = $1`, conn.ID()).Returns(1)

	conn2, err := models.GetCallByID(ctx, db, testdata.Org1.ID, conn.ID())
	assert.NoError(t, err)
	assert.Equal(t, "test1", conn2.ExternalID())
}
