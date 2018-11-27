package hooks

import (
	"testing"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/actions"
	"github.com/nyaruka/mailroom/models"
)

func TestContactFieldChanged(t *testing.T) {
	genderUUID := models.FieldUUID("e25f711c-7460-4cd5-9461-39744a1feade")
	gender := assets.NewFieldReference("gender", "Gender")

	ageUUID := models.FieldUUID("4e83c89a-31f1-4628-bb4b-8f035a515145")
	age := assets.NewFieldReference("age", "Age")

	tcs := []HookTestCase{
		HookTestCase{
			Actions: ContactActionMap{
				Cathy: []flows.Action{
					actions.NewSetContactFieldAction(newActionUUID(), gender, "Male"),
					actions.NewSetContactFieldAction(newActionUUID(), gender, "Female"),
					actions.NewSetContactFieldAction(newActionUUID(), age, ""),
				},
				Evan: []flows.Action{
					actions.NewSetContactFieldAction(newActionUUID(), gender, "Male"),
					actions.NewSetContactFieldAction(newActionUUID(), gender, ""),
					actions.NewSetContactFieldAction(newActionUUID(), age, "30"),
				},
				Bob: []flows.Action{
					actions.NewSetContactFieldAction(newActionUUID(), gender, ""),
					actions.NewSetContactFieldAction(newActionUUID(), gender, "Male"),
					actions.NewSetContactFieldAction(newActionUUID(), age, "Old"),
				},
			},
			SQLAssertions: []SQLAssertion{
				SQLAssertion{
					SQL:   `select count(*) from contacts_contact where id = $1 AND fields->$2 = '{"text":"Female"}'::jsonb`,
					Args:  []interface{}{Cathy, genderUUID},
					Count: 1,
				},
				SQLAssertion{
					SQL:   `select count(*) from contacts_contact where id = $1 AND NOT fields?$2`,
					Args:  []interface{}{Cathy, ageUUID},
					Count: 1,
				},
				SQLAssertion{
					SQL:   `select count(*) from contacts_contact where id = $1 AND NOT fields?$2`,
					Args:  []interface{}{Evan, genderUUID},
					Count: 1,
				},
				SQLAssertion{
					SQL:   `select count(*) from contacts_contact where id = $1 AND fields->$2 = '{"text":"30", "number": 30}'::jsonb`,
					Args:  []interface{}{Evan, ageUUID},
					Count: 1,
				},
				SQLAssertion{
					SQL:   `select count(*) from contacts_contact where id = $1 AND fields->$2 = '{"text":"Male"}'::jsonb`,
					Args:  []interface{}{Bob, genderUUID},
					Count: 1,
				},
				SQLAssertion{
					SQL:   `select count(*) from contacts_contact where id = $1 AND fields->$2 = '{"text":"Old"}'::jsonb`,
					Args:  []interface{}{Bob, ageUUID},
					Count: 1,
				},
				SQLAssertion{
					SQL:   `select count(*) from contacts_contact where id = $1 AND NOT fields?$2`,
					Args:  []interface{}{Bob, "unknown"},
					Count: 1,
				},
			},
		},
	}

	RunActionTestCases(t, tcs)
}
