package hooks

import (
	"testing"

	"github.com/greatnonprofits-nfp/goflow/flows"
	"github.com/greatnonprofits-nfp/goflow/flows/actions"
	"github.com/nyaruka/mailroom/models"
)

func TestContactNameChanged(t *testing.T) {
	tcs := []HookTestCase{
		HookTestCase{
			Actions: ContactActionMap{
				models.CathyID: []flows.Action{
					actions.NewSetContactNameAction(newActionUUID(), "Fred"),
					actions.NewSetContactNameAction(newActionUUID(), "Tarzan"),
				},
				models.GeorgeID: []flows.Action{
					actions.NewSetContactNameAction(newActionUUID(), "Geoff Newman"),
				},
				models.AlexandriaID: []flows.Action{
					actions.NewSetContactNameAction(newActionUUID(), "😃234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"),
				},
			},
			SQLAssertions: []SQLAssertion{
				SQLAssertion{
					SQL:   "select count(*) from contacts_contact where name = 'Tarzan' and id = $1",
					Args:  []interface{}{models.CathyID},
					Count: 1,
				},
				SQLAssertion{
					SQL:   "select count(*) from contacts_contact where name = 'Tarzan'",
					Count: 1,
				},
				SQLAssertion{
					SQL:   "select count(*) from contacts_contact where name = 'Bob' and id = $1",
					Args:  []interface{}{models.BobID},
					Count: 1,
				},
				SQLAssertion{
					SQL:   "select count(*) from contacts_contact where name = 'Geoff Newman' and id = $1",
					Args:  []interface{}{models.GeorgeID},
					Count: 1,
				},
				SQLAssertion{
					SQL:   "select count(*) from contacts_contact where name = '😃2345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678' and id = $1",
					Args:  []interface{}{models.AlexandriaID},
					Count: 1,
				},
			},
		},
	}

	RunActionTestCases(t, tcs)
}
