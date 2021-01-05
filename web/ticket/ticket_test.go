package ticket

import (
	"testing"

	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/models"
	_ "github.com/nyaruka/mailroom/services/tickets/mailgun"
	_ "github.com/nyaruka/mailroom/services/tickets/zendesk"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/nyaruka/mailroom/web"
)

func TestTicketClose(t *testing.T) {
	testsuite.Reset()
	db := testsuite.DB()

	// create 2 open tickets and 1 closed one for Cathy
	testdata.InsertOpenTicket(t, db, models.Org1, models.CathyID, models.MailgunID, flows.TicketUUID(uuids.New()), "Need help", "Have you seen my cookies?", "17")
	testdata.InsertOpenTicket(t, db, models.Org1, models.CathyID, models.ZendeskID, flows.TicketUUID(uuids.New()), "More help", "Have you seen my cookies?", "21")
	testdata.InsertClosedTicket(t, db, models.Org1, models.CathyID, models.ZendeskID, flows.TicketUUID(uuids.New()), "Old question", "Have you seen my cookies?", "34")

	web.RunWebTests(t, "testdata/close.json")
}

func TestTicketReopen(t *testing.T) {
	testsuite.Reset()
	db := testsuite.DB()

	// create 2 closed tickets and 1 open one for Cathy
	testdata.InsertClosedTicket(t, db, models.Org1, models.CathyID, models.MailgunID, flows.TicketUUID(uuids.New()), "Need help", "Have you seen my cookies?", "17")
	testdata.InsertClosedTicket(t, db, models.Org1, models.CathyID, models.ZendeskID, flows.TicketUUID(uuids.New()), "More help", "Have you seen my cookies?", "21")
	testdata.InsertOpenTicket(t, db, models.Org1, models.CathyID, models.ZendeskID, flows.TicketUUID(uuids.New()), "Old question", "Have you seen my cookies?", "34")

	web.RunWebTests(t, "testdata/reopen.json")
}
