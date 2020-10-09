package handlers_test

import (
	"testing"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/actions"
	"github.com/nyaruka/mailroom/core/handlers"
	"github.com/nyaruka/mailroom/core/models"
)

func TestContactLanguageChanged(t *testing.T) {
	tcs := []handlers.TestCase{
		{
			Actions: handlers.ContactActionMap{
				models.CathyID: []flows.Action{
					actions.NewSetContactLanguage(handlers.NewActionUUID(), "fra"),
					actions.NewSetContactLanguage(handlers.NewActionUUID(), "eng"),
				},
				models.GeorgeID: []flows.Action{
					actions.NewSetContactLanguage(handlers.NewActionUUID(), "spa"),
				},
				models.AlexandriaID: []flows.Action{
					actions.NewSetContactLanguage(handlers.NewActionUUID(), ""),
				},
			},
			SQLAssertions: []handlers.SQLAssertion{
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

	handlers.RunTestCases(t, tcs)
}
