package mailgun

import (
	"testing"

	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/nyaruka/mailroom/web"
)

func TestReceive(t *testing.T) {
	testsuite.Reset()
	db := testsuite.DB()

	db.MustExec(`DELETE FROM msgs_msg`)

	// create a mailgun ticket for Cathy
	ticket := testdata.InsertOpenTicket(db, testdata.Org1, testdata.Cathy, testdata.Mailgun, "Need help", "Have you seen my cookies?", "", nil)

	web.RunWebTests(t, "testdata/receive.json", map[string]string{"cathy_ticket_uuid": string(ticket.UUID)})
}
