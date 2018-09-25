package handlers

import (
	"testing"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/testsuite"
)

func TestCampaigns(t *testing.T) {
	testsuite.Reset()

	//	joinedUUID := models.FieldUUID("8c1c1256-78d6-4a5b-9f1c-1761d5728251")
	joined := assets.NewFieldReference("joined", "Joined")
	//doctors := assets.NewGroupReference(assets.GroupUUID("85a5a793-4741-4896-b55e-05af65f3c0fa"), "Doctors")
	doctorsID := models.GroupID(33)

	// add cathy and bob as doctors
	testsuite.DB().MustExec("insert into contacts_contactgroup_contacts(contact_id, contactgroup_id) VALUES($1, $2);", Cathy, doctorsID)
	testsuite.DB().MustExec("insert into contacts_contactgroup_contacts(contact_id, contactgroup_id) VALUES($1, $2);", Bob, doctorsID)

	tcs := []EventTestCase{
		EventTestCase{
			Events: ContactEventMap{
				Cathy: []flows.Event{
					events.NewContactFieldChangedEvent(joined, "2029-09-15T12:00:00+00:00"),
					events.NewContactFieldChangedEvent(joined, ""),
				},
				Bob: []flows.Event{
					events.NewContactFieldChangedEvent(joined, "2029-09-15T12:00:00+00:00"),
					events.NewContactFieldChangedEvent(joined, "2029-09-15T15:00:00+00:00"),
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
			},
		},
	}

	RunEventTestCases(t, tcs)
}
