package msg_test

import (
	"fmt"
	"testing"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/nyaruka/mailroom/web"
)

func TestServer(t *testing.T) {
	_, _, db, _ := testsuite.Get()

	cathyIn := testdata.InsertIncomingMsg(db, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "hello", models.MsgStatusHandled)
	cathyOut := testdata.InsertOutgoingMsg(db, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "how can we help", nil, models.MsgStatusSent)
	bobOut := testdata.InsertOutgoingMsg(db, testdata.Org1, testdata.VonageChannel, testdata.Bob, "this failed", nil, models.MsgStatusFailed)

	web.RunWebTests(t, "testdata/resend.json", map[string]string{
		"cathy_msgin_id":  fmt.Sprintf("%d", cathyIn.ID()),
		"cathy_msgout_id": fmt.Sprintf("%d", cathyOut.ID()),
		"bob_msgout_id":   fmt.Sprintf("%d", bobOut.ID()),
	})
}
