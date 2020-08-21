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
	db.Exec(`UPDATE contacts_contact SET status = 'A', is_blocked = FALSE, is_stopped = FALSE WHERE id = $1`, models.CathyID)

	tcs := []HookTestCase{
		{
			Modifiers: ContactModifierMap{
				models.CathyID: []flows.Modifier{modifiers.NewStatus(flows.ContactStatusBlocked)},
			},
			SQLAssertions: []SQLAssertion{
				{
					SQL:   `select count(*) from contacts_contact where id = $1 AND status = 'B' AND is_blocked = TRUE`,
					Args:  []interface{}{models.CathyID},
					Count: 1,
				},
			},
		},
		{
			Modifiers: ContactModifierMap{
				models.CathyID: []flows.Modifier{modifiers.NewStatus(flows.ContactStatusStopped)},
			},
			SQLAssertions: []SQLAssertion{
				{
					SQL:   `select count(*) from contacts_contact where id = $1 AND status = 'S' AND is_stopped = TRUE`,
					Args:  []interface{}{models.CathyID},
					Count: 1,
				},
			},
		},
		{
			Modifiers: ContactModifierMap{
				models.CathyID: []flows.Modifier{modifiers.NewStatus(flows.ContactStatusActive)},
			},
			SQLAssertions: []SQLAssertion{
				{
					SQL:   `select count(*) from contacts_contact where id = $1 AND status = 'A' AND is_stopped = FALSE`,
					Args:  []interface{}{models.CathyID},
					Count: 1,
				},
				{
					SQL:   `select count(*) from contacts_contact where id = $1 AND status = 'A' AND is_blocked = FALSE`,
					Args:  []interface{}{models.CathyID},
					Count: 1,
				},
			},
		},
	}

	RunHookTestCases(t, tcs)
}
