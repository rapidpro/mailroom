package hooks

import (
	"testing"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/actions"
)

func TestContactNameChanged(t *testing.T) {
	tcs := []HookTestCase{
		HookTestCase{
			Actions: ContactActionMap{
				Cathy: []flows.Action{
					actions.NewSetContactNameAction(newActionUUID(), "Fred"),
					actions.NewSetContactNameAction(newActionUUID(), "Tarzan"),
				},
				Evan: []flows.Action{
					actions.NewSetContactNameAction(newActionUUID(), "Geoff Newman"),
				},
			},
			Assertions: []SQLAssertion{
				SQLAssertion{
					SQL:   "select count(*) from contacts_contact where name = 'Tarzan' and id = $1",
					Args:  []interface{}{Cathy},
					Count: 1,
				},
				SQLAssertion{
					SQL:   "select count(*) from contacts_contact where name = 'Tarzan'",
					Count: 1,
				},
				SQLAssertion{
					SQL:   "select count(*) from contacts_contact where name = 'Bob Newman' and id = $1",
					Args:  []interface{}{Bob},
					Count: 1,
				},
				SQLAssertion{
					SQL:   "select count(*) from contacts_contact where name = 'Geoff Newman' and id = $1",
					Args:  []interface{}{Evan},
					Count: 1,
				},
			},
		},
	}

	RunActionTestCases(t, tcs)
}
