package twilioflex_test

import (
	"log"
	"testing"

	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/nyaruka/mailroom/web"
)

func TestEventCallback(t *testing.T) {
	ctx, rt, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetData | testsuite.ResetStorage)

	ticket := testdata.InsertOpenTicket(
		db,
		testdata.Org1,
		testdata.Cathy,
		testdata.Twilioflex,
		testdata.DefaultTopic,
		"Have you seen my cookies?",
		"CH6442c09c93ba4d13966fa42e9b78f620",
		nil,
	)

	log.Println(string(ticket.UUID))
	web.RunWebTests(t, ctx, rt, "testdata/event_callback.json", map[string]string{"cathy_ticket_uuid": string(ticket.UUID)})
}
