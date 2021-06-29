package mailgun

import (
	"testing"

	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/nyaruka/mailroom/web"
)

func TestReceive(t *testing.T) {
	_, _, db, _ := testsuite.Reset()

	defer func() {
		db.MustExec(`DELETE FROM msgs_msg`)
		db.MustExec(`DELETE FROM tickets_ticketevent`)
		db.MustExec(`DELETE FROM tickets_ticket`)
	}()

	// create a mailgun ticket for Cathy
	ticket := testdata.InsertOpenTicket(db, testdata.Org1, testdata.Cathy, testdata.Mailgun, "Need help", "Have you seen my cookies?", "", nil)

	web.RunWebTests(t, "testdata/receive.json", map[string]string{"cathy_ticket_uuid": string(ticket.UUID)})
}
