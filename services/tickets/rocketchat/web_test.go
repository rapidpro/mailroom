package rocketchat_test

import (
	"testing"

	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/nyaruka/mailroom/web"
)

func TestEventCallback(t *testing.T) {
	_, _, db, _ := testsuite.Get()

	defer testsuite.ResetData(db)

	// create a rocketchat ticket for Cathy
	ticket := testdata.InsertOpenTicket(db, testdata.Org1, testdata.Cathy, testdata.RocketChat, testdata.DefaultTopic, "Have you seen my cookies?", "1234", nil)

	web.RunWebTests(t, "testdata/event_callback.json", map[string]string{"cathy_ticket_uuid": string(ticket.UUID)})
}
