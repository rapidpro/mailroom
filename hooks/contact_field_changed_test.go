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
				models.Cathy: []flows.Action{
					actions.NewSetContactFieldAction(newActionUUID(), gender, "Male"),
					actions.NewSetContactFieldAction(newActionUUID(), gender, "Female"),
					actions.NewSetContactFieldAction(newActionUUID(), age, ""),
				},
				models.Evan: []flows.Action{
					actions.NewSetContactFieldAction(newActionUUID(), gender, "Male"),
					actions.NewSetContactFieldAction(newActionUUID(), gender, ""),
					actions.NewSetContactFieldAction(newActionUUID(), age, "30"),
				},
				models.Bob: []flows.Action{
					actions.NewSetContactFieldAction(newActionUUID(), gender, ""),
					actions.NewSetContactFieldAction(newActionUUID(), gender, "Male"),
					actions.NewSetContactFieldAction(newActionUUID(), age, "Old"),
				},
			},
			SQLAssertions: []SQLAssertion{
				SQLAssertion{
					SQL:   `select count(*) from contacts_contact where id = $1 AND fields->$2 = '{"text":"Female"}'::jsonb`,
					Args:  []interface{}{models.Cathy, genderUUID},
					Count: 1,
				},
				SQLAssertion{
					SQL:   `select count(*) from contacts_contact where id = $1 AND NOT fields?$2`,
					Args:  []interface{}{models.Cathy, ageUUID},
					Count: 1,
				},
				SQLAssertion{
					SQL:   `select count(*) from contacts_contact where id = $1 AND NOT fields?$2`,
					Args:  []interface{}{models.Evan, genderUUID},
					Count: 1,
				},
				SQLAssertion{
					SQL:   `select count(*) from contacts_contact where id = $1 AND fields->$2 = '{"text":"30", "number": 30}'::jsonb`,
					Args:  []interface{}{models.Evan, ageUUID},
					Count: 1,
				},
				SQLAssertion{
					SQL:   `select count(*) from contacts_contact where id = $1 AND fields->$2 = '{"text":"Male"}'::jsonb`,
					Args:  []interface{}{models.Bob, genderUUID},
					Count: 1,
				},
				SQLAssertion{
					SQL:   `select count(*) from contacts_contact where id = $1 AND fields->$2 = '{"text":"Old"}'::jsonb`,
					Args:  []interface{}{models.Bob, ageUUID},
					Count: 1,
				},
				SQLAssertion{
					SQL:   `select count(*) from contacts_contact where id = $1 AND NOT fields?$2`,
					Args:  []interface{}{models.Bob, "unknown"},
					Count: 1,
				},
			},
		},
	}

	RunActionTestCases(t, tcs)
}
