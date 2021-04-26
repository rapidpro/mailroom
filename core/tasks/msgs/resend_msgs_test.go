package msgs_test

import (
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
	msgOut3 := testdata.InsertOutgoingMsg(t, db, models.Org1, models.CathyID, models.CathyURN, models.CathyURNID, "out 3", nil)

	db.MustExec(`UPDATE msgs_msg SET metadata = '{"topic":"cool-stuff"}' WHERE id = $1`, msgOut1.ID())

	// create our task
	task := &msgs.ResendMsgsTask{
		MsgIDs: []models.MsgID{models.MsgID(msgOut1.ID()), models.MsgID(msgOut2.ID())},
	}

	// execute it
	err := task.Perform(ctx, mr, models.Org1)
	require.NoError(t, err)

	// there should be 2 new pending outgoing messages in the database, with channel set
	testsuite.AssertQueryCount(t, db,
		`SELECT count(*) FROM msgs_msg WHERE direction = 'O' AND status = 'P' AND channel_id IS NOT NULL AND id > $1`,
		[]interface{}{msgOut3.ID()}, 2,
	)

	// cloning will have cloned message text, attachments and metadata
	testsuite.AssertQueryCount(t, db,
		`SELECT count(*) FROM msgs_msg WHERE text = 'out 1' AND attachments = '{"image/jpeg:hi.jpg"}' AND metadata = '{"topic":"cool-stuff"}' AND id != $1`,
		[]interface{}{msgOut1.ID()}, 1,
	)

	// the resent messages should have had their status updated
	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM msgs_msg WHERE status = 'R'`, nil, 2)
}
