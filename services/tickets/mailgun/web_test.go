package mailgun

import (
	"testing"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/nyaruka/mailroom/web"
)

func TestReceive(t *testing.T) {
	testsuite.Reset()
	db := testsuite.DB()

	// create a mailgun ticket for Cathy
	testdata.InsertOpenTicket(t, db, models.Org1, models.CathyID, models.MailgunID, "c69f103c-db64-4481-815b-1112890419ef", "Need help", "Have you seen my cookies?", "")

	web.RunWebTests(t, "testdata/receive.json")
}
