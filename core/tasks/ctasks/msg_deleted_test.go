package ctasks_test

import (
	"testing"
	"time"

	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/tasks/ctasks"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMsgDeleted(t *testing.T) {
	ctx, rt := testsuite.Runtime(t)

	defer testsuite.Reset(t, rt, testsuite.ResetData|testsuite.ResetDynamo)

	oa := testdb.Org1.Load(t, rt)

	ann, _, _ := testdb.Ann.Load(t, rt, oa)

	testdb.InsertIncomingMsg(t, rt, testdb.Org1, "0199c4cb-f111-7ce8-9ce9-614d61a2c198", testdb.TwilioChannel, testdb.Ann, "hello", models.MsgStatusHandled)
	testdb.InsertIncomingMsg(t, rt, testdb.Org1, "0199c4cf-486a-79af-9892-79254b6ac5b7", testdb.TwilioChannel, testdb.Ann, "goodbye", models.MsgStatusHandled)

	task := &ctasks.MsgDeleted{
		MsgUUID: "0199c4cb-f111-7ce8-9ce9-614d61a2c198",
	}

	err := task.Perform(ctx, rt, oa, ann)
	assert.NoError(t, err)

	assertdb.Query(t, rt.DB, `SELECT uuid::text, visibility FROM msgs_msg`).Map(map[string]any{
		"0199c4cb-f111-7ce8-9ce9-614d61a2c198": "X",
		"0199c4cf-486a-79af-9892-79254b6ac5b7": "V",
	})

	items := testsuite.GetHistoryItems(t, rt, false, time.Time{})
	if assert.Equal(t, 2, len(items)) {
		assert.Equal(t, "con#a393abc0-283d-4c9b-a1b3-641a035c34bf", items[0].PK)
		assert.Equal(t, "evt#0199c4cb-f111-7ce8-9ce9-614d61a2c198#del", items[0].SK)

		data, err := items[0].GetData()
		require.NoError(t, err)
		assert.Equal(t, true, data["by_contact"])

		assert.Equal(t, "con#a393abc0-283d-4c9b-a1b3-641a035c34bf", items[1].PK)
		assert.Regexp(t, "evt#[a-z0-9\\-]{36}", items[1].SK)

		data, err = items[1].GetData()
		require.NoError(t, err)
		assert.Equal(t, "msg_deleted", data["type"])
	}
}
