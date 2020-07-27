package hooks

import (
	"testing"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/modifiers"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/testsuite"
)

func TestContactStatusChanged(t *testing.T) {

	db := testsuite.DB()

	// make sure cathyID contact is active
	db.Exec(`UPDATE contacts_contact SET is_blocked = FALSE WHERE id = $1`, models.CathyID)
	db.Exec(`UPDATE contacts_contact SET is_stopped = FALSE WHERE id = $1`, models.CathyID)

	tcs := []HookTestCase{
		HookTestCase{
			Modifiers: ContactModifierMap{
				models.CathyID: []flows.Modifier{modifiers.NewStatus(flows.ContactStatusBlocked)},
			},
			SQLAssertions: []SQLAssertion{
				SQLAssertion{
					SQL:   `select count(*) from contacts_contact where id = $1 AND is_blocked = TRUE`,
					Args:  []interface{}{models.CathyID},
					Count: 1,
				},
			},
		},
		HookTestCase{
			Modifiers: ContactModifierMap{
				models.CathyID: []flows.Modifier{modifiers.NewStatus(flows.ContactStatusStopped)},
			},
			SQLAssertions: []SQLAssertion{
				SQLAssertion{
					SQL:   `select count(*) from contacts_contact where id = $1 AND is_stopped = TRUE`,
					Args:  []interface{}{models.CathyID},
					Count: 1,
				},
			},
		},
		HookTestCase{
			Modifiers: ContactModifierMap{
				models.CathyID: []flows.Modifier{modifiers.NewStatus(flows.ContactStatusActive)},
			},
			SQLAssertions: []SQLAssertion{
				SQLAssertion{
					SQL:   `select count(*) from contacts_contact where id = $1 AND is_stopped = FALSE`,
					Args:  []interface{}{models.CathyID},
					Count: 1,
				},
				SQLAssertion{
					SQL:   `select count(*) from contacts_contact where id = $1 AND is_blocked = FALSE`,
					Args:  []interface{}{models.CathyID},
					Count: 1,
				},
			},
		},
	}

	RunHookTestCases(t, tcs)
}
