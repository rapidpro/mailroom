package msg_test

import (
	"fmt"
	"testing"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
)

func TestSend(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetData)

	testsuite.RunWebTests(t, ctx, rt, "testdata/send.json", nil)
}

func TestResend(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetData)

	cathyIn := testdata.InsertIncomingMsg(rt, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "hello", models.MsgStatusHandled)
	cathyOut := testdata.InsertOutgoingMsg(rt, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "how can we help", nil, models.MsgStatusSent, false)
	bobOut := testdata.InsertOutgoingMsg(rt, testdata.Org1, testdata.VonageChannel, testdata.Bob, "this failed", nil, models.MsgStatusFailed, false)
	georgeOut := testdata.InsertOutgoingMsg(rt, testdata.Org1, testdata.VonageChannel, testdata.George, "no URN", nil, models.MsgStatusFailed, false)
	rt.DB.MustExec(`UPDATE msgs_msg SET contact_urn_id = NULL WHERE id = $1`, georgeOut.ID())

	testsuite.RunWebTests(t, ctx, rt, "testdata/resend.json", map[string]string{
		"cathy_msgin_id":   fmt.Sprintf("%d", cathyIn.ID()),
		"cathy_msgout_id":  fmt.Sprintf("%d", cathyOut.ID()),
		"bob_msgout_id":    fmt.Sprintf("%d", bobOut.ID()),
		"george_msgout_id": fmt.Sprintf("%d", georgeOut.ID()),
	})
}
