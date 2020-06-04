package zendesk

import (
	"testing"

	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/web"
)

func TestChannelback(t *testing.T) {
	testsuite.Reset()
	db := testsuite.DB()

	// create a zendesk ticket for Cathy
	db.MustExec(`INSERT INTO tickets_ticket(id, uuid, org_id, contact_id, ticketer_id, status, subject, body, opened_on, modified_on)
	VALUES(1, 'c69f103c-db64-4481-815b-1112890419ef', $1, $2, $3, 'O', 'Need help', 'Have you seen my cookies?', NOW(), NOW())`, models.Org1, models.CathyID, models.ZendeskID)

	web.RunWebTests(t, "testdata/channelback.json")
}

func TestEventCallback(t *testing.T) {
	testsuite.Reset()
	db := testsuite.DB()

	// create a zendesk ticket for Cathy
	db.MustExec(`INSERT INTO tickets_ticket(id, uuid, org_id, contact_id, ticketer_id, status, subject, body, opened_on, modified_on)
	VALUES(1, 'c69f103c-db64-4481-815b-1112890419ef', $1, $2, $3, 'O', 'Need help', 'Have you seen my cookies?', NOW(), NOW())`, models.Org1, models.CathyID, models.ZendeskID)

	web.RunWebTests(t, "testdata/event_callback.json")
}

func TestTarget(t *testing.T) {
	testsuite.Reset()
	db := testsuite.DB()

	// create a zendesk ticket for Cathy
	db.MustExec(`INSERT INTO tickets_ticket(id, uuid, external_id, org_id, contact_id, ticketer_id, status, subject, body, opened_on, modified_on)
	VALUES(1, 'c69f103c-db64-4481-815b-1112890419ef', '1234', $1, $2, $3, 'O', 'Need help', 'Have you seen my cookies?', NOW(), NOW())`, models.Org1, models.CathyID, models.ZendeskID)

	web.RunWebTests(t, "testdata/target.json")
}
