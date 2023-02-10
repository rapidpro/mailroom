package rocketchat_test

import (
	"testing"
	"time"

	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
)

func TestEventCallback(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetData | testsuite.ResetStorage)

	// create a rocketchat ticket for Cathy
	ticket := testdata.InsertOpenTicket(rt, testdata.Org1, testdata.Cathy, testdata.RocketChat, testdata.DefaultTopic, "Have you seen my cookies?", "1234", time.Now(), nil)

	testsuite.RunWebTests(t, ctx, rt, "testdata/event_callback.json", map[string]string{"cathy_ticket_uuid": string(ticket.UUID)})
}
