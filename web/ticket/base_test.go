package ticket

import (
	"testing"
	"time"

	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
)

func TestTicketAssign(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetData)

	testdata.InsertOpenTicket(rt, testdata.Org1, testdata.Cathy, testdata.DefaultTopic, "Have you seen my cookies?", time.Now(), testdata.Admin)
	testdata.InsertOpenTicket(rt, testdata.Org1, testdata.Cathy, testdata.DefaultTopic, "Have you seen my cookies?", time.Now(), testdata.Agent)
	testdata.InsertClosedTicket(rt, testdata.Org1, testdata.Cathy, testdata.DefaultTopic, "Have you seen my cookies?", nil)
	testdata.InsertClosedTicket(rt, testdata.Org1, testdata.Bob, testdata.DefaultTopic, "", nil)

	testsuite.RunWebTests(t, ctx, rt, "testdata/assign.json", nil)
}

func TestTicketAddNote(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetData)

	testdata.InsertOpenTicket(rt, testdata.Org1, testdata.Cathy, testdata.DefaultTopic, "Have you seen my cookies?", time.Now(), testdata.Admin)
	testdata.InsertOpenTicket(rt, testdata.Org1, testdata.Cathy, testdata.DefaultTopic, "Have you seen my cookies?", time.Now(), testdata.Agent)
	testdata.InsertClosedTicket(rt, testdata.Org1, testdata.Cathy, testdata.DefaultTopic, "Have you seen my cookies?", nil)

	testsuite.RunWebTests(t, ctx, rt, "testdata/add_note.json", nil)
}

func TestTicketChangeTopic(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetData)

	testdata.InsertOpenTicket(rt, testdata.Org1, testdata.Cathy, testdata.DefaultTopic, "Have you seen my cookies?", time.Now(), testdata.Admin)
	testdata.InsertOpenTicket(rt, testdata.Org1, testdata.Cathy, testdata.SupportTopic, "Have you seen my cookies?", time.Now(), testdata.Agent)
	testdata.InsertClosedTicket(rt, testdata.Org1, testdata.Cathy, testdata.SalesTopic, "Have you seen my cookies?", nil)

	testsuite.RunWebTests(t, ctx, rt, "testdata/change_topic.json", nil)
}

func TestTicketClose(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetData)

	// create 2 open tickets and 1 closed one for Cathy
	testdata.InsertOpenTicket(rt, testdata.Org1, testdata.Cathy, testdata.DefaultTopic, "Have you seen my cookies?", time.Now(), testdata.Admin)
	testdata.InsertOpenTicket(rt, testdata.Org1, testdata.Cathy, testdata.DefaultTopic, "Have you seen my cookies?", time.Now(), nil)
	testdata.InsertClosedTicket(rt, testdata.Org1, testdata.Cathy, testdata.DefaultTopic, "Have you seen my cookies?", testdata.Editor)

	testsuite.RunWebTests(t, ctx, rt, "testdata/close.json", nil)
}

func TestTicketReopen(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetData | testsuite.ResetRedis)

	// we should be able to reopen ticket #1 because Cathy has no other tickets open
	testdata.InsertClosedTicket(rt, testdata.Org1, testdata.Cathy, testdata.DefaultTopic, "Have you seen my cookies?", testdata.Admin)

	// but then we won't be able to open ticket #2
	testdata.InsertClosedTicket(rt, testdata.Org1, testdata.Cathy, testdata.DefaultTopic, "Have you seen my cookies?", nil)

	testdata.InsertClosedTicket(rt, testdata.Org1, testdata.Bob, testdata.DefaultTopic, "Have you seen my cookies?", testdata.Editor)
	testdata.InsertClosedTicket(rt, testdata.Org1, testdata.Alexandria, testdata.DefaultTopic, "Have you seen my cookies?", testdata.Editor)

	testsuite.RunWebTests(t, ctx, rt, "testdata/reopen.json", nil)
}
