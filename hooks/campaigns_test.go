package hooks

import (
	"testing"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/excellent/types"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/testsuite"
)

func testCampaigns(t *testing.T) {
	testsuite.Reset()

	//	joinedUUID := models.FieldUUID("8c1c1256-78d6-4a5b-9f1c-1761d5728251")
	joined := assets.NewFieldReference("joined", "Joined")
	//doctors := assets.NewGroupReference(assets.GroupUUID("85a5a793-4741-4896-b55e-05af65f3c0fa"), "Doctors")
	doctorsID := models.GroupID(33)

	// add cathy and bob as doctors
	testsuite.DB().MustExec("insert into contacts_contactgroup_contacts(contact_id, contactgroup_id) VALUES($1, $2);", Cathy, doctorsID)
	testsuite.DB().MustExec("insert into contacts_contactgroup_contacts(contact_id, contactgroup_id) VALUES($1, $2);", Bob, doctorsID)

	// init their values
	testsuite.DB().MustExec(
		`update contacts_contact set fields = fields - '8c1c1256-78d6-4a5b-9f1c-1761d5728251'
		WHERE id = $1`, Cathy)

	testsuite.DB().MustExec(
		`update contacts_contact set fields = fields ||
		'{"8c1c1256-78d6-4a5b-9f1c-1761d5728251": { "text": "2029-09-15T12:00:00+00:00", "datetime": "2029-09-15T12:00:00+00:00" }}'::jsonb
		WHERE id = $1`, Bob)

	tcs := []EventTestCase{
		EventTestCase{
			Events: ContactEventMap{
				Cathy: []flows.Event{
					events.NewContactFieldChangedEvent(joined, &flows.Value{Text: types.NewXText("2029-09-15T12:00:00+00:00")}),
					events.NewContactFieldChangedEvent(joined, &flows.Value{}),
				},
				Bob: []flows.Event{
					events.NewContactFieldChangedEvent(joined, &flows.Value{Text: types.NewXText("2029-09-15T12:00:00+00:00")}),
					events.NewContactFieldChangedEvent(joined, &flows.Value{Text: types.NewXText("2029-09-15T12:00:00+00:00")}),
				},
				Evan: []flows.Event{
					events.NewContactFieldChangedEvent(joined, &flows.Value{Text: types.NewXText("2029-09-15T12:00:00+00:00")}),
				},
			},
			Assertions: []SQLAssertion{
				SQLAssertion{
					SQL:   `select count(*) FROM campaigns_eventfire WHERE contact_id = $1`,
					Args:  []interface{}{Cathy},
					Count: 0,
				},
				SQLAssertion{
					SQL:   `select count(*) FROM campaigns_eventfire WHERE contact_id = $1`,
					Args:  []interface{}{Bob},
					Count: 2,
				},
				SQLAssertion{
					SQL:   `select count(*) FROM campaigns_eventfire WHERE contact_id = $1`,
					Args:  []interface{}{Evan},
					Count: 0,
				},
			},
		},
	}

	RunEventTestCases(t, tcs)
}
