package ticket

import (
	"testing"

	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/mailroom/models"
	_ "github.com/nyaruka/mailroom/services/tickets/mailgun"
	_ "github.com/nyaruka/mailroom/services/tickets/zendesk"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/web"
)

func TestTicketClose(t *testing.T) {
	testsuite.Reset()
	db := testsuite.DB()

	// create 2 open tickets and 1 closed one for Cathy
	db.MustExec(`INSERT INTO tickets_ticket(id, uuid, external_id, org_id, contact_id, ticketer_id, status, subject, body, opened_on, modified_on)
	VALUES(1, $1, '17', $2, $3, $4, 'O', 'Need help', 'Have you seen my cookies?', NOW(), NOW())`, uuids.New(), models.Org1, models.CathyID, models.MailgunID)

	db.MustExec(`INSERT INTO tickets_ticket(id, uuid, external_id, org_id, contact_id, ticketer_id, status, subject, body, opened_on, modified_on)
	VALUES(2, $1, '21', $2, $3, $4, 'O', 'More help', 'Have you seen my cookies?', NOW(), NOW())`, uuids.New(), models.Org1, models.CathyID, models.ZendeskID)

	db.MustExec(`INSERT INTO tickets_ticket(id, uuid, external_id, org_id, contact_id, ticketer_id, status, subject, body, opened_on, modified_on, closed_on)
	VALUES(3, $1, '34', $2, $3, $4, 'C', 'Old question', 'Have you seen my cookies?', NOW(), NOW(), NOW())`, uuids.New(), models.Org1, models.CathyID, models.ZendeskID)

	web.RunWebTests(t, "testdata/close.json")
}

func TestTicketReopen(t *testing.T) {
	testsuite.Reset()
	db := testsuite.DB()

	// create 2 closed tickets and 1 open one for Cathy
	db.MustExec(`INSERT INTO tickets_ticket(id, uuid, external_id, org_id, contact_id, ticketer_id, status, subject, body, opened_on, modified_on, closed_on)
	VALUES(1, $1, '17', $2, $3, $4, 'C', 'Need help', 'Have you seen my cookies?', NOW(), NOW(), NOW())`, uuids.New(), models.Org1, models.CathyID, models.MailgunID)

	db.MustExec(`INSERT INTO tickets_ticket(id, uuid, external_id, org_id, contact_id, ticketer_id, status, subject, body, opened_on, modified_on, closed_on)
	VALUES(2, $1, '21', $2, $3, $4, 'C', 'More help', 'Have you seen my cookies?', NOW(), NOW(), NOW())`, uuids.New(), models.Org1, models.CathyID, models.ZendeskID)

	db.MustExec(`INSERT INTO tickets_ticket(id, uuid, external_id, org_id, contact_id, ticketer_id, status, subject, body, opened_on, modified_on)
	VALUES(3, $1, '34', $2, $3, $4, 'O', 'Old question', 'Have you seen my cookies?', NOW(), NOW())`, uuids.New(), models.Org1, models.CathyID, models.ZendeskID)

	web.RunWebTests(t, "testdata/reopen.json")
}
