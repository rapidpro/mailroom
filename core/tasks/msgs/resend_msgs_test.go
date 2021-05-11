package msgs_test

import (
	"fmt"
	"testing"

	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/config"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/tasks/msgs"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"

	"github.com/stretchr/testify/require"
)

func TestResendMsgs(t *testing.T) {
	testsuite.Reset()
	ctx := testsuite.CTX()
	db := testsuite.DB()
	mr := &mailroom.Mailroom{Config: config.Mailroom, DB: db, RP: testsuite.RP(), ElasticClient: nil}

	msgOut1 := testdata.InsertOutgoingMsg(t, db, models.Org1, models.CathyID, models.CathyURN, models.CathyURNID, "out 1", []utils.Attachment{"image/jpeg:hi.jpg"})
	msgOut2 := testdata.InsertOutgoingMsg(t, db, models.Org1, models.BobID, models.BobURN, models.BobURNID, "out 2", nil)
	testdata.InsertOutgoingMsg(t, db, models.Org1, models.CathyID, models.CathyURN, models.CathyURNID, "out 3", nil)

	db.MustExec(`UPDATE msgs_msg SET metadata = '{"topic":"cool-stuff"}' WHERE id = $1`, msgOut1.ID())

	// create our task
	task := &msgs.ResendMsgsTask{
		MsgIDs: []models.MsgID{models.MsgID(msgOut1.ID()), models.MsgID(msgOut2.ID())},
	}

	// execute it
	err := task.Perform(ctx, mr, models.Org1)
	require.NoError(t, err)

	// the two resent messages should now be pending and have a channel set
	testsuite.AssertQueryCount(t, db,
		`SELECT count(*) FROM msgs_msg WHERE status = 'P' AND channel_id IS NOT NULL AND id IN ($1, $2)`,
		[]interface{}{msgOut1.ID(), msgOut2.ID()}, 2,
	)

	testsuite.AssertCourierQueues(t, map[string][]int{
		fmt.Sprintf("msgs:%s|10/0", models.TwilioChannelUUID): {1, 1},
	}, "courier queue mismatch after resending")
}
