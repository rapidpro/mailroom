package zendesk

import (
	"testing"
	"time"

	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
)

func TestChannelback(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetData)

	// create a zendesk ticket for Cathy
	ticket := testdata.InsertOpenTicket(rt, testdata.Org1, testdata.Cathy, testdata.Zendesk, testdata.DefaultTopic, "Have you seen my cookies?", "1234", time.Now(), nil)

	testsuite.RunWebTests(t, ctx, rt, "testdata/channelback.json", map[string]string{"cathy_ticket_uuid": string(ticket.UUID)})
}

func TestEventCallback(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetAll) // tests include destroying ticketer

	// create a zendesk ticket for Cathy
	ticket := testdata.InsertOpenTicket(rt, testdata.Org1, testdata.Cathy, testdata.Zendesk, testdata.DefaultTopic, "Have you seen my cookies?", "1234", time.Now(), nil)

	testsuite.RunWebTests(t, ctx, rt, "testdata/event_callback.json", map[string]string{"cathy_ticket_uuid": string(ticket.UUID)})
}

func TestTarget(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetData)

	// create a zendesk ticket for Cathy
	ticket := testdata.InsertOpenTicket(rt, testdata.Org1, testdata.Cathy, testdata.Zendesk, testdata.DefaultTopic, "Have you seen my cookies?", "1234", time.Now(), nil)

	testsuite.RunWebTests(t, ctx, rt, "testdata/target.json", map[string]string{"cathy_ticket_uuid": string(ticket.UUID)})
}
