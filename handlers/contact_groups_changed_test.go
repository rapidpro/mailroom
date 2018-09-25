package handlers

import (
	"testing"

	"github.com/nyaruka/goflow/assets"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/models"
)

func TestContactGroupsChanged(t *testing.T) {
	doctors := assets.NewGroupReference(assets.GroupUUID("85a5a793-4741-4896-b55e-05af65f3c0fa"), "Doctors")
	doctorsID := models.GroupID(33)

	teachers := assets.NewGroupReference(assets.GroupUUID("27ea3e9c-497c-4f13-aaa5-bf768fea1597"), "Teachers")
	teachersID := models.GroupID(34)

	unknown := assets.NewGroupReference(assets.GroupUUID("cab5fd6b-caea-42ea-8f5e-7796f04225e2"), "Unknown")

	tcs := []EventTestCase{
		EventTestCase{
			Events: ContactEventMap{
				Cathy: []flows.Event{
					events.NewContactGroupsAddedEvent([]*assets.GroupReference{doctors}),
					events.NewContactGroupsAddedEvent([]*assets.GroupReference{doctors}),
					events.NewContactGroupsRemovedEvent([]*assets.GroupReference{doctors}),
					events.NewContactGroupsAddedEvent([]*assets.GroupReference{teachers}),
				},
				Evan: []flows.Event{
					events.NewContactGroupsRemovedEvent([]*assets.GroupReference{doctors}),
					events.NewContactGroupsAddedEvent([]*assets.GroupReference{teachers}),
					events.NewContactGroupsRemovedEvent([]*assets.GroupReference{unknown}),
					events.NewContactGroupsAddedEvent([]*assets.GroupReference{unknown}),
				},
			},
			Assertions: []SQLAssertion{
				SQLAssertion{
					SQL:   "select count(*) from contacts_contactgroup_contacts where contact_id = $1 and contactgroup_id = $2",
					Args:  []interface{}{Cathy, doctorsID},
					Count: 0,
				},
				SQLAssertion{
					SQL:   "select count(*) from contacts_contactgroup_contacts where contact_id = $1 and contactgroup_id = $2",
					Args:  []interface{}{Cathy, teachersID},
					Count: 1,
				},
				SQLAssertion{
					SQL:   "select count(*) from contacts_contactgroup_contacts where contact_id = $1 and contactgroup_id = $2",
					Args:  []interface{}{Evan, teachersID},
					Count: 1,
				},
				SQLAssertion{
					SQL:   "select count(*) from contacts_contactgroup_contacts where contact_id = $1 and contactgroup_id = $2",
					Args:  []interface{}{Bob, teachersID},
					Count: 0,
				},
			},
		},
	}

	RunEventTestCases(t, tcs)
}
