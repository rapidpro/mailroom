package hooks

import (
	"testing"

	"github.com/nyaruka/goflow/assets"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/actions"
	"github.com/nyaruka/mailroom/models"
)

func TestContactGroupsChanged(t *testing.T) {
	doctors := assets.NewGroupReference(assets.GroupUUID("85a5a793-4741-4896-b55e-05af65f3c0fa"), "Doctors")
	doctorsID := models.GroupID(33)

	teachers := assets.NewGroupReference(assets.GroupUUID("27ea3e9c-497c-4f13-aaa5-bf768fea1597"), "Teachers")
	teachersID := models.GroupID(34)

	tcs := []HookTestCase{
		HookTestCase{
			Actions: ContactActionMap{
				Cathy: []flows.Action{
					actions.NewAddContactGroupsAction(newActionUUID(), []*assets.GroupReference{doctors}),
					actions.NewAddContactGroupsAction(newActionUUID(), []*assets.GroupReference{doctors}),
					actions.NewRemoveContactGroupsAction(newActionUUID(), []*assets.GroupReference{doctors}, false),
					actions.NewAddContactGroupsAction(newActionUUID(), []*assets.GroupReference{teachers}),
				},
				Evan: []flows.Action{
					actions.NewRemoveContactGroupsAction(newActionUUID(), []*assets.GroupReference{doctors}, false),
					actions.NewAddContactGroupsAction(newActionUUID(), []*assets.GroupReference{teachers}),
				},
			},
			SQLAssertions: []SQLAssertion{
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

	RunActionTestCases(t, tcs)
}
