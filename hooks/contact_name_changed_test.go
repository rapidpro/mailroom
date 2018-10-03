package hooks

import (
	"testing"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
)

func TestContactNameChanged(t *testing.T) {
	tcs := []EventTestCase{
		EventTestCase{
			Events: ContactEventMap{
				Cathy: []flows.Event{
					events.NewContactNameChangedEvent("Fred"),
					events.NewContactNameChangedEvent("Tarzan"),
				},
				Evan: []flows.Event{
					events.NewContactNameChangedEvent("Geoff Newman"),
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

	RunEventTestCases(t, tcs)
}
