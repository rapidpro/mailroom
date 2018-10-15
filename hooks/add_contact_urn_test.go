package hooks

import (
	"testing"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/actions"
)

func TestAddContactURN(t *testing.T) {
	tcs := []HookTestCase{
		HookTestCase{
			Actions: ContactActionMap{
				Cathy: []flows.Action{
					actions.NewAddContactURNAction(newActionUUID(), "tel", "12065551212"),
					actions.NewAddContactURNAction(newActionUUID(), "tel", "12065551212"),
					actions.NewAddContactURNAction(newActionUUID(), "telegram", "11551"),
				},
				Evan: []flows.Action{},
			},
			Assertions: []SQLAssertion{
				SQLAssertion{
					SQL:   "select count(*) from contacts_contacturn where contact_id = $1 and scheme = 'telegram' and path = '11551' and priority = -1",
					Args:  []interface{}{Cathy},
					Count: 1,
				},
				SQLAssertion{
					SQL:   "select count(*) from contacts_contacturn where contact_id = $1 and scheme = 'tel' and path = '12065551212' and priority = -1 and identity = 'tel:12065551212'",
					Args:  []interface{}{Cathy},
					Count: 1,
				},
				SQLAssertion{
					SQL:   "select count(*) from contacts_contacturn where contact_id = $1",
					Args:  []interface{}{Bob},
					Count: 1,
				},
			},
		},
	}

	RunActionTestCases(t, tcs)
}
