package hooks

import (
	"testing"

	"github.com/nyaruka/goflow/assets"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/actions"
	"github.com/nyaruka/mailroom/models"
)

func TestContactGroupsChanged(t *testing.T) {
	doctors := assets.NewGroupReference(models.DoctorsGroupUUID, "Doctors")
	testers := assets.NewGroupReference(models.TestersGroupUUID, "Testers")

	tcs := []HookTestCase{
		HookTestCase{
			Actions: ContactActionMap{
				models.CathyID: []flows.Action{
					actions.NewAddContactGroups(newActionUUID(), []*assets.GroupReference{doctors}),
					actions.NewAddContactGroups(newActionUUID(), []*assets.GroupReference{doctors}),
					actions.NewRemoveContactGroups(newActionUUID(), []*assets.GroupReference{doctors}, false),
					actions.NewAddContactGroups(newActionUUID(), []*assets.GroupReference{testers}),
				},
				models.GeorgeID: []flows.Action{
					actions.NewRemoveContactGroups(newActionUUID(), []*assets.GroupReference{doctors}, false),
					actions.NewAddContactGroups(newActionUUID(), []*assets.GroupReference{testers}),
				},
			},
			SQLAssertions: []SQLAssertion{
				{
					SQL:   "select count(*) from contacts_contactgroup_contacts where contact_id = $1 and contactgroup_id = $2",
					Args:  []interface{}{models.CathyID, models.DoctorsGroupID},
					Count: 0,
				},
				{
					SQL:   "select count(*) from contacts_contactgroup_contacts where contact_id = $1 and contactgroup_id = $2",
					Args:  []interface{}{models.CathyID, models.TestersGroupID},
					Count: 1,
				},
				{
					SQL:   "select count(*) from contacts_contactgroup_contacts where contact_id = $1 and contactgroup_id = $2",
					Args:  []interface{}{models.GeorgeID, models.TestersGroupID},
					Count: 1,
				},
				{
					SQL:   "select count(*) from contacts_contactgroup_contacts where contact_id = $1 and contactgroup_id = $2",
					Args:  []interface{}{models.BobID, models.TestersGroupID},
					Count: 0,
				},
			},
		},
	}

	RunHookTestCases(t, tcs)
}
