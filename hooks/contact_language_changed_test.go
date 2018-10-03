package hooks

import (
	"testing"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
)

func TestContactLanguageChanged(t *testing.T) {
	tcs := []EventTestCase{
		EventTestCase{
			Events: ContactEventMap{
				Cathy: []flows.Event{
					events.NewContactLanguageChangedEvent("fra"),
					events.NewContactLanguageChangedEvent("eng"),
				},
				Evan: []flows.Event{
					events.NewContactLanguageChangedEvent("spa"),
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

	RunEventTestCases(t, tcs)
}
