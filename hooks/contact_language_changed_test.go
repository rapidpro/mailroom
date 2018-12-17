package hooks

import (
	"testing"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/actions"
	"github.com/nyaruka/mailroom/models"
)

func TestContactLanguageChanged(t *testing.T) {
	tcs := []HookTestCase{
		HookTestCase{
			Actions: ContactActionMap{
				models.Cathy: []flows.Action{
					actions.NewSetContactLanguageAction(newActionUUID(), "fra"),
					actions.NewSetContactLanguageAction(newActionUUID(), "eng"),
				},
				models.Evan: []flows.Action{
					actions.NewSetContactLanguageAction(newActionUUID(), "spa"),
				},
			},
			SQLAssertions: []SQLAssertion{
				SQLAssertion{
					SQL:   "select count(*) from contacts_contact where id = $1 and language = 'eng'",
					Args:  []interface{}{models.Cathy},
					Count: 1,
				},
				SQLAssertion{
					SQL:   "select count(*) from contacts_contact where id = $1 and language = 'spa'",
					Args:  []interface{}{models.Evan},
					Count: 1,
				},
				SQLAssertion{
					SQL:   "select count(*) from contacts_contact where id = $1 and language is NULL;",
					Args:  []interface{}{models.Bob},
					Count: 1,
				},
			},
		},
	}

	RunActionTestCases(t, tcs)
}
