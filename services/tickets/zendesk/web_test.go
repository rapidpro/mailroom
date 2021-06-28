package zendesk

import (
	"testing"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/nyaruka/mailroom/web"
)

func TestChannelback(t *testing.T) {
	testsuite.Reset()
	db := testsuite.DB()

	// create a zendesk ticket for Cathy
	testdata.InsertOpenTicket(t, db, models.Org1, models.CathyID, models.ZendeskID, "c69f103c-db64-4481-815b-1112890419ef", "Need help", "Have you seen my cookies?", "1234")

	web.RunWebTests(t, "testdata/channelback.json")
}

func TestEventCallback(t *testing.T) {
	testsuite.Reset()
	db := testsuite.DB()

	// create a zendesk ticket for Cathy
	testdata.InsertOpenTicket(t, db, models.Org1, models.CathyID, models.ZendeskID, "c69f103c-db64-4481-815b-1112890419ef", "Need help", "Have you seen my cookies?", "1234")

	web.RunWebTests(t, "testdata/event_callback.json")
}

func TestTarget(t *testing.T) {
	testsuite.Reset()
	db := testsuite.DB()

	// create a zendesk ticket for Cathy
	testdata.InsertOpenTicket(t, db, models.Org1, models.CathyID, models.ZendeskID, "c69f103c-db64-4481-815b-1112890419ef", "Need help", "Have you seen my cookies?", "1234")

	web.RunWebTests(t, "testdata/target.json")
}
