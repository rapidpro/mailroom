package rocketchat_test

import (
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/nyaruka/mailroom/web"
	"testing"
)

func TestEventCallback(t *testing.T) {
	testsuite.Reset()
	db := testsuite.DB()

	// create a rocketchat ticket for Cathy
	testdata.InsertOpenTicket(t, db, models.Org1, models.CathyID, models.RocketChatID, "c69f103c-db64-4481-815b-1112890419ef", "Need help", "Have you seen my cookies?", "1234")

	web.RunWebTests(t, "testdata/event_callback.json")
}
