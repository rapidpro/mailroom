package hooks

import (
	"testing"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/actions"
	"github.com/nyaruka/mailroom/models"
)

func TestContactNameChanged(t *testing.T) {
	tcs := []HookTestCase{
		{
			Actions: ContactActionMap{
				models.CathyID: []flows.Action{
					actions.NewSetContactName(newActionUUID(), "Fred"),
					actions.NewSetContactName(newActionUUID(), "Tarzan"),
				},
				models.GeorgeID: []flows.Action{
					actions.NewSetContactName(newActionUUID(), "Geoff Newman"),
				},
				models.BobID: []flows.Action{
					actions.NewSetContactName(newActionUUID(), ""),
				},
				models.AlexandriaID: []flows.Action{
					actions.NewSetContactName(newActionUUID(), "😃234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"),
				},
			},
			SQLAssertions: []SQLAssertion{
				{
					SQL:   "select count(*) from contacts_contact where name = 'Tarzan' and id = $1",
					Args:  []interface{}{models.CathyID},
					Count: 1,
				},
				{
					SQL:   "select count(*) from contacts_contact where name = 'Tarzan'",
					Count: 1,
				},
				{
					SQL:   "select count(*) from contacts_contact where name IS NULL and id = $1",
					Args:  []interface{}{models.BobID},
					Count: 1,
				},
				{
					SQL:   "select count(*) from contacts_contact where name = 'Geoff Newman' and id = $1",
					Args:  []interface{}{models.GeorgeID},
					Count: 1,
				},
				{
					SQL:   "select count(*) from contacts_contact where name = '😃2345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678' and id = $1",
					Args:  []interface{}{models.AlexandriaID},
					Count: 1,
				},
			},
		},
	}

	RunHookTestCases(t, tcs)
}
