package hooks

import (
	"testing"

	"github.com/greatnonprofits-nfp/goflow/flows"
	"github.com/greatnonprofits-nfp/goflow/flows/actions"
	"github.com/nyaruka/mailroom/models"
)

func TestContactLanguageChanged(t *testing.T) {
	tcs := []HookTestCase{
		HookTestCase{
			Actions: ContactActionMap{
				models.CathyID: []flows.Action{
					actions.NewSetContactLanguage(newActionUUID(), "fra"),
					actions.NewSetContactLanguage(newActionUUID(), "eng"),
				},
				models.GeorgeID: []flows.Action{
					actions.NewSetContactLanguage(newActionUUID(), "spa"),
				},
			},
			SQLAssertions: []SQLAssertion{
				SQLAssertion{
					SQL:   "select count(*) from contacts_contact where id = $1 and language = 'eng'",
					Args:  []interface{}{models.CathyID},
					Count: 1,
				},
				SQLAssertion{
					SQL:   "select count(*) from contacts_contact where id = $1 and language = 'spa'",
					Args:  []interface{}{models.GeorgeID},
					Count: 1,
				},
				SQLAssertion{
					SQL:   "select count(*) from contacts_contact where id = $1 and language is NULL;",
					Args:  []interface{}{models.BobID},
					Count: 1,
				},
			},
		},
	}

	RunActionTestCases(t, tcs)
}
