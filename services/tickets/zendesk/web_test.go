package zendesk

import (
	"testing"

	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/nyaruka/mailroom/web"
)

func TestChannelback(t *testing.T) {
	_, _, db, _ := testsuite.Reset()

	// create a zendesk ticket for Cathy
	ticket := testdata.InsertOpenTicket(db, testdata.Org1, testdata.Cathy, testdata.Zendesk, "Need help", "Have you seen my cookies?", "1234", nil)

	web.RunWebTests(t, "testdata/channelback.json", map[string]string{"cathy_ticket_uuid": string(ticket.UUID)})
}

func TestEventCallback(t *testing.T) {
	_, _, db, _ := testsuite.Reset()

	// create a zendesk ticket for Cathy
	ticket := testdata.InsertOpenTicket(db, testdata.Org1, testdata.Cathy, testdata.Zendesk, "Need help", "Have you seen my cookies?", "1234", nil)

	web.RunWebTests(t, "testdata/event_callback.json", map[string]string{"cathy_ticket_uuid": string(ticket.UUID)})
}

func TestTarget(t *testing.T) {
	_, _, db, _ := testsuite.Reset()

	// create a zendesk ticket for Cathy
	ticket := testdata.InsertOpenTicket(db, testdata.Org1, testdata.Cathy, testdata.Zendesk, "Need help", "Have you seen my cookies?", "1234", nil)

	web.RunWebTests(t, "testdata/target.json", map[string]string{"cathy_ticket_uuid": string(ticket.UUID)})
}
