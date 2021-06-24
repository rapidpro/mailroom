package rocketchat_test

import (
	"testing"

	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/nyaruka/mailroom/web"
)

func TestEventCallback(t *testing.T) {
	testsuite.Reset()
	db := testsuite.DB()

	db.MustExec(`DELETE FROM msgs_msg`)

	// create a rocketchat ticket for Cathy
	ticket := testdata.InsertOpenTicket(db, testdata.Org1, testdata.Cathy, testdata.RocketChat, "Need help", "Have you seen my cookies?", "1234", nil)

	web.RunWebTests(t, "testdata/event_callback.json", map[string]string{"cathy_ticket_uuid": string(ticket.UUID)})
}
