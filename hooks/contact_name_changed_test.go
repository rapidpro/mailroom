package hooks_test

import (
	"testing"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/actions"
	"github.com/nyaruka/mailroom/hooks"
	"github.com/nyaruka/mailroom/models"
)

func TestContactNameChanged(t *testing.T) {
	tcs := []hooks.TestCase{
		{
			Actions: hooks.ContactActionMap{
				models.CathyID: []flows.Action{
					actions.NewSetContactName(hooks.NewActionUUID(), "Fred"),
					actions.NewSetContactName(hooks.NewActionUUID(), "Tarzan"),
				},
				models.GeorgeID: []flows.Action{
					actions.NewSetContactName(hooks.NewActionUUID(), "Geoff Newman"),
				},
				models.BobID: []flows.Action{
					actions.NewSetContactName(hooks.NewActionUUID(), ""),
				},
				models.AlexandriaID: []flows.Action{
					actions.NewSetContactName(hooks.NewActionUUID(), "😃234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"),
				},
			},
			SQLAssertions: []hooks.SQLAssertion{
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

	hooks.RunTestCases(t, tcs)
}
