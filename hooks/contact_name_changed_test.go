package hooks

import (
	"testing"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/actions"
	"github.com/nyaruka/mailroom/models"
)

func TestContactNameChanged(t *testing.T) {
	tcs := []HookTestCase{
		HookTestCase{
			Actions: ContactActionMap{
				models.Cathy: []flows.Action{
					actions.NewSetContactNameAction(newActionUUID(), "Fred"),
					actions.NewSetContactNameAction(newActionUUID(), "Tarzan"),
				},
				models.Evan: []flows.Action{
					actions.NewSetContactNameAction(newActionUUID(), "Geoff Newman"),
				},
			},
			SQLAssertions: []SQLAssertion{
				SQLAssertion{
					SQL:   "select count(*) from contacts_contact where name = 'Tarzan' and id = $1",
					Args:  []interface{}{models.Cathy},
					Count: 1,
				},
				SQLAssertion{
					SQL:   "select count(*) from contacts_contact where name = 'Tarzan'",
					Count: 1,
				},
				SQLAssertion{
					SQL:   "select count(*) from contacts_contact where name = 'Bob Newman' and id = $1",
					Args:  []interface{}{models.Bob},
					Count: 1,
				},
				SQLAssertion{
					SQL:   "select count(*) from contacts_contact where name = 'Geoff Newman' and id = $1",
					Args:  []interface{}{models.Evan},
					Count: 1,
				},
			},
		},
	}

	RunActionTestCases(t, tcs)
}
