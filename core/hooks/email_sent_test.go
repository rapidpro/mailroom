package hooks_test

import (
	"testing"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/actions"
	"github.com/nyaruka/mailroom/core/hooks"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
)

func TestEmailSent(t *testing.T) {
	// configure mailtrap for our org
	db := testsuite.DB()
	db.MustExec(`UPDATE orgs_org SET config = '{"smtp_server": "smtp://24f335c64dbc28:d7966a553e76f6@smtp.mailtrap.io:2525/?from=mailroom@foo.bar"}' WHERE id = 1;`)

	tcs := []hooks.TestCase{
		{
			Actions: hooks.ContactActionMap{
				models.CathyID: []flows.Action{
					actions.NewSendEmail(hooks.NewActionUUID(), []string{"cathy@foo.bar", "bob@foo.bar"}, "Test Email", "This is your test email"),
				},
			},
			SQLAssertions: []hooks.SQLAssertion{},
		},
	}
	hooks.RunTestCases(t, tcs)
}
