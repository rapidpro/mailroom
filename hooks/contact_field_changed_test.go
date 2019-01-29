package hooks

import (
	"testing"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/actions"
	"github.com/nyaruka/mailroom/models"
)

func TestContactFieldChanged(t *testing.T) {
	gender := assets.NewFieldReference("gender", "Gender")
	age := assets.NewFieldReference("age", "Age")

	tcs := []HookTestCase{
		HookTestCase{
			Actions: ContactActionMap{
				models.CathyID: []flows.Action{
					actions.NewSetContactFieldAction(newActionUUID(), gender, "Male"),
					actions.NewSetContactFieldAction(newActionUUID(), gender, "Female"),
					actions.NewSetContactFieldAction(newActionUUID(), age, ""),
				},
				models.GeorgeID: []flows.Action{
					actions.NewSetContactFieldAction(newActionUUID(), gender, "Male"),
					actions.NewSetContactFieldAction(newActionUUID(), gender, ""),
					actions.NewSetContactFieldAction(newActionUUID(), age, "40"),
				},
				models.BobID: []flows.Action{
					actions.NewSetContactFieldAction(newActionUUID(), gender, ""),
					actions.NewSetContactFieldAction(newActionUUID(), gender, "Male"),
					actions.NewSetContactFieldAction(newActionUUID(), age, "Old"),
				},
			},
			SQLAssertions: []SQLAssertion{
				SQLAssertion{
					SQL:   `select count(*) from contacts_contact where id = $1 AND fields->$2 = '{"text":"Female"}'::jsonb`,
					Args:  []interface{}{models.CathyID, models.GenderFieldUUID},
					Count: 1,
				},
				SQLAssertion{
					SQL:   `select count(*) from contacts_contact where id = $1 AND NOT fields?$2`,
					Args:  []interface{}{models.CathyID, models.AgeFieldUUID},
					Count: 1,
				},
				SQLAssertion{
					SQL:   `select count(*) from contacts_contact where id = $1 AND NOT fields?$2`,
					Args:  []interface{}{models.GeorgeID, models.GenderFieldUUID},
					Count: 1,
				},
				SQLAssertion{
					SQL:   `select count(*) from contacts_contact where id = $1 AND fields->$2 = '{"text":"40", "number": 40}'::jsonb`,
					Args:  []interface{}{models.GeorgeID, models.AgeFieldUUID},
					Count: 1,
				},
				SQLAssertion{
					SQL:   `select count(*) from contacts_contact where id = $1 AND fields->$2 = '{"text":"Male"}'::jsonb`,
					Args:  []interface{}{models.BobID, models.GenderFieldUUID},
					Count: 1,
				},
				SQLAssertion{
					SQL:   `select count(*) from contacts_contact where id = $1 AND fields->$2 = '{"text":"Old"}'::jsonb`,
					Args:  []interface{}{models.BobID, models.AgeFieldUUID},
					Count: 1,
				},
				SQLAssertion{
					SQL:   `select count(*) from contacts_contact where id = $1 AND NOT fields?$2`,
					Args:  []interface{}{models.BobID, "unknown"},
					Count: 1,
				},
			},
		},
	}

	RunActionTestCases(t, tcs)
}
