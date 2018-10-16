package hooks

import (
	"testing"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/actions"
)

func TestContactLanguageChanged(t *testing.T) {
	tcs := []HookTestCase{
		HookTestCase{
			Actions: ContactActionMap{
				Cathy: []flows.Action{
					actions.NewSetContactLanguageAction(newActionUUID(), "fra"),
					actions.NewSetContactLanguageAction(newActionUUID(), "eng"),
				},
				Evan: []flows.Action{
					actions.NewSetContactLanguageAction(newActionUUID(), "spa"),
				},
			},
			Assertions: []SQLAssertion{
				SQLAssertion{
					SQL:   "select count(*) from contacts_contact where id = $1 and language = 'eng'",
					Args:  []interface{}{Cathy},
					Count: 1,
				},
				SQLAssertion{
					SQL:   "select count(*) from contacts_contact where id = $1 and language = 'spa'",
					Args:  []interface{}{Evan},
					Count: 1,
				},
				SQLAssertion{
					SQL:   "select count(*) from contacts_contact where id = $1 and language is NULL;",
					Args:  []interface{}{Bob},
					Count: 1,
				},
			},
		},
	}

	RunActionTestCases(t, tcs)
}
