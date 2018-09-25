package handlers

import (
	"testing"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/models"
)

func TestContactFieldChanged(t *testing.T) {
	genderUUID := models.FieldUUID("0d345143-1863-4688-93c9-e392c0e77954")
	gender := assets.NewFieldReference("gender", "Gender")

	ageUUID := models.FieldUUID("042c13fe-e0b1-4426-9192-2681758c2619")
	age := assets.NewFieldReference("age", "Age")

	unknown := assets.NewFieldReference("unknown", "unknown")

	// TODO: add other field types when done

	tcs := []EventTestCase{
		EventTestCase{
			Events: ContactEventMap{
				Cathy: []flows.Event{
					events.NewContactFieldChangedEvent(gender, "Male"),
					events.NewContactFieldChangedEvent(gender, "Female"),
					events.NewContactFieldChangedEvent(age, ""),
				},
				Evan: []flows.Event{
					events.NewContactFieldChangedEvent(gender, "Male"),
					events.NewContactFieldChangedEvent(gender, ""),
				},
				Bob: []flows.Event{
					events.NewContactFieldChangedEvent(gender, ""),
					events.NewContactFieldChangedEvent(gender, "Male"),
					events.NewContactFieldChangedEvent(age, "Old"),
					events.NewContactFieldChangedEvent(unknown, "unknown"),
				},
			},
			Assertions: []SQLAssertion{
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
					SQL:   `select count(*) from contacts_contact where id = $1 AND fields->$2 = '{"text":"Male"}'::jsonb`,
					Args:  []interface{}{Bob, genderUUID},
					Count: 1,
				},
				// TODO: this should have numeric values in our test
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

	RunEventTestCases(t, tcs)
}
