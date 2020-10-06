package hooks_test

import (
	"testing"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/actions"
	"github.com/nyaruka/mailroom/core/hooks"
	"github.com/nyaruka/mailroom/core/models"
)

func TestContactLanguageChanged(t *testing.T) {
	tcs := []hooks.TestCase{
		{
			Actions: hooks.ContactActionMap{
				models.CathyID: []flows.Action{
					actions.NewSetContactLanguage(hooks.NewActionUUID(), "fra"),
					actions.NewSetContactLanguage(hooks.NewActionUUID(), "eng"),
				},
				models.GeorgeID: []flows.Action{
					actions.NewSetContactLanguage(hooks.NewActionUUID(), "spa"),
				},
				models.AlexandriaID: []flows.Action{
					actions.NewSetContactLanguage(hooks.NewActionUUID(), ""),
				},
			},
			SQLAssertions: []hooks.SQLAssertion{
				{
					SQL:   "select count(*) from contacts_contact where id = $1 and language = 'eng'",
					Args:  []interface{}{models.CathyID},
					Count: 1,
				},
				{
					SQL:   "select count(*) from contacts_contact where id = $1 and language = 'spa'",
					Args:  []interface{}{models.GeorgeID},
					Count: 1,
				},
				{
					SQL:   "select count(*) from contacts_contact where id = $1 and language is NULL;",
					Args:  []interface{}{models.BobID},
					Count: 1,
				},
				{
					SQL:   "select count(*) from contacts_contact where id = $1 and language is NULL;",
					Args:  []interface{}{models.AlexandriaID},
					Count: 1,
				},
			},
		},
	}

	hooks.RunTestCases(t, tcs)
}
