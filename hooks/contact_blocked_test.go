package hooks

import (
	"testing"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/actions/modifiers"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/testsuite"
)

func TestContactBlocked(t *testing.T) {

	db := testsuite.DB()

	// make sure cathyID contact is not blocked
	db.Exec(`UPDATE contacts_contact SET is_blocked = FALSE WHERE id = $1`, models.CathyID)
	tcs := []HookTestCase{
		HookTestCase{
			Modifiers: ContactModifierMap{
				models.CathyID: []flows.Modifier{modifiers.NewBlocked(true)},
			},
			SQLAssertions: []SQLAssertion{
				SQLAssertion{
					SQL:   `select count(*) from contacts_contact where id = $1 AND is_blocked = TRUE`,
					Args:  []interface{}{models.CathyID},
					Count: 1,
				},
			},
		},
	}

	RunHookTestCases(t, tcs)
}
