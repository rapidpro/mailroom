package hooks

import (
	"testing"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/actions"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/testsuite"
)

func TestContactFieldChanged(t *testing.T) {
	gender := assets.NewFieldReference("gender", "Gender")
	age := assets.NewFieldReference("age", "Age")

	db := testsuite.DB()

	// populate some field values on alexandria
	db.Exec(`UPDATE contacts_contact SET fields = '{"903f51da-2717-47c7-a0d3-f2f32877013d": {"text":"34"}, "3a5891e4-756e-4dc9-8e12-b7a766168824": {"text":"female"}}' WHERE id = $1`, models.AlexandriaID)

	tcs := []HookTestCase{
		HookTestCase{
			Actions: ContactActionMap{
				models.CathyID: []flows.Action{
					actions.NewSetContactField(newActionUUID(), gender, "Male"),
					actions.NewSetContactField(newActionUUID(), gender, "Female"),
					actions.NewSetContactField(newActionUUID(), age, ""),
				},
				models.GeorgeID: []flows.Action{
					actions.NewSetContactField(newActionUUID(), gender, "Male"),
					actions.NewSetContactField(newActionUUID(), gender, ""),
					actions.NewSetContactField(newActionUUID(), age, "40"),
				},
				models.BobID: []flows.Action{
					actions.NewSetContactField(newActionUUID(), gender, ""),
					actions.NewSetContactField(newActionUUID(), gender, "Male"),
					actions.NewSetContactField(newActionUUID(), age, "Old"),
				},
				models.AlexandriaID: []flows.Action{
					actions.NewSetContactField(newActionUUID(), age, ""),
					actions.NewSetContactField(newActionUUID(), gender, ""),
				},
			},
			SQLAssertions: []SQLAssertion{
				{
					SQL:   `select count(*) from contacts_contact where id = $1 AND fields->$2 = '{"text":"Female"}'::jsonb`,
					Args:  []interface{}{models.CathyID, models.GenderFieldUUID},
					Count: 1,
				},
				{
					SQL:   `select count(*) from contacts_contact where id = $1 AND NOT fields?$2`,
					Args:  []interface{}{models.CathyID, models.AgeFieldUUID},
					Count: 1,
				},
				{
					SQL:   `select count(*) from contacts_contact where id = $1 AND NOT fields?$2`,
					Args:  []interface{}{models.GeorgeID, models.GenderFieldUUID},
					Count: 1,
				},
				{
					SQL:   `select count(*) from contacts_contact where id = $1 AND fields->$2 = '{"text":"40", "number": 40}'::jsonb`,
					Args:  []interface{}{models.GeorgeID, models.AgeFieldUUID},
					Count: 1,
				},
				{
					SQL:   `select count(*) from contacts_contact where id = $1 AND fields->$2 = '{"text":"Male"}'::jsonb`,
					Args:  []interface{}{models.BobID, models.GenderFieldUUID},
					Count: 1,
				},
				{
					SQL:   `select count(*) from contacts_contact where id = $1 AND fields->$2 = '{"text":"Old"}'::jsonb`,
					Args:  []interface{}{models.BobID, models.AgeFieldUUID},
					Count: 1,
				},
				{
					SQL:   `select count(*) from contacts_contact where id = $1 AND NOT fields?$2`,
					Args:  []interface{}{models.BobID, "unknown"},
					Count: 1,
				},
				{
					SQL:   `select count(*) from contacts_contact where id = $1 AND fields = '{}'`,
					Args:  []interface{}{models.AlexandriaID},
					Count: 1,
				},
			},
		},
	}

	RunHookTestCases(t, tcs)
}
