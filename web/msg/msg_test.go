package msg_test

import (
	"fmt"
	"testing"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/nyaruka/mailroom/web"
)

func TestResendMessages(t *testing.T) {
	ctx, rt, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetData)

	cathyIn := testdata.InsertIncomingMsg(db, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "hello", models.MsgStatusHandled)
	cathyOut := testdata.InsertOutgoingMsg(db, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "how can we help", nil, models.MsgStatusSent, false)
	bobOut := testdata.InsertOutgoingMsg(db, testdata.Org1, testdata.VonageChannel, testdata.Bob, "this failed", nil, models.MsgStatusFailed, false)
	georgeOut := testdata.InsertOutgoingMsg(db, testdata.Org1, testdata.VonageChannel, testdata.George, "no URN", nil, models.MsgStatusFailed, false)
	db.MustExec(`UPDATE msgs_msg SET contact_urn_id = NULL WHERE id = $1`, georgeOut.ID())

	web.RunWebTests(t, ctx, rt, "testdata/resend.json", map[string]string{
		"cathy_msgin_id":   fmt.Sprintf("%d", cathyIn.ID()),
		"cathy_msgout_id":  fmt.Sprintf("%d", cathyOut.ID()),
		"bob_msgout_id":    fmt.Sprintf("%d", bobOut.ID()),
		"george_msgout_id": fmt.Sprintf("%d", georgeOut.ID()),
	})
}

func TestFailMessages(t *testing.T) {
	ctx, rt, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetData)

	cathyIn := testdata.InsertIncomingMsg(db, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "hello", models.MsgStatusHandled)
	cathyOut := testdata.InsertOutgoingMsg(db, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "how can we help", nil, models.MsgStatusSent, false)
	bobOut := testdata.InsertOutgoingMsg(db, testdata.Org1, testdata.VonageChannel, testdata.Bob, "this failed", nil, models.MsgStatusQueued, false)
	georgeOut := testdata.InsertOutgoingMsg(db, testdata.Org1, testdata.VonageChannel, testdata.George, "no URN", nil, models.MsgStatusPending, false)
	chrisOut := testdata.InsertOutgoingMsg(db, testdata.Org1, testdata.VonageChannel, testdata.George, "no URN", nil, models.MsgStatusErrored, false)
	danOut := testdata.InsertOutgoingMsg(db, testdata.Org1, testdata.VonageChannel, testdata.George, "no URN", nil, models.MsgStatusFailed, false)

	web.RunWebTests(t, ctx, rt, "testdata/fail.json", map[string]string{
		"cathy_msgin_id":   fmt.Sprintf("%d", cathyIn.ID()),
		"cathy_msgout_id":  fmt.Sprintf("%d", cathyOut.ID()),
		"bob_msgout_id":    fmt.Sprintf("%d", bobOut.ID()),
		"george_msgout_id": fmt.Sprintf("%d", georgeOut.ID()),
		"chris_msgout_id":  fmt.Sprintf("%d", chrisOut.ID()),
		"dan_msgout_id":    fmt.Sprintf("%d", danOut.ID()),
	})
}
