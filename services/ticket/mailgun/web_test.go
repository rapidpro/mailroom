package mailgun

import (
	"testing"

	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/web"
)

func TestReceive(t *testing.T) {
	testsuite.Reset()
	db := testsuite.DB()

	// create a mailgun ticket for Cathy
	db.MustExec(`INSERT INTO tickets_ticket(id, uuid,  org_id, contact_id, ticketer_id, status, subject, body, opened_on, modified_on)
	VALUES(1, $1, $2, $3, $4, 'O', 'Need help', 'Have you seen my cookies?', NOW(), NOW())`, "c69f103c-db64-4481-815b-1112890419ef", models.Org1, models.CathyID, models.MailgunID)

	web.RunWebTests(t, "testdata/receive.json")
}
