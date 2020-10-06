package hooks_test

import (
	"testing"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/actions"
	"github.com/nyaruka/mailroom/core/hooks"
	"github.com/nyaruka/mailroom/core/models"
)

func TestContactGroupsChanged(t *testing.T) {
	doctors := assets.NewGroupReference(models.DoctorsGroupUUID, "Doctors")
	testers := assets.NewGroupReference(models.TestersGroupUUID, "Testers")

	tcs := []hooks.TestCase{
		{
			Actions: hooks.ContactActionMap{
				models.CathyID: []flows.Action{
					actions.NewAddContactGroups(hooks.NewActionUUID(), []*assets.GroupReference{doctors}),
					actions.NewAddContactGroups(hooks.NewActionUUID(), []*assets.GroupReference{doctors}),
					actions.NewRemoveContactGroups(hooks.NewActionUUID(), []*assets.GroupReference{doctors}, false),
					actions.NewAddContactGroups(hooks.NewActionUUID(), []*assets.GroupReference{testers}),
				},
				models.GeorgeID: []flows.Action{
					actions.NewRemoveContactGroups(hooks.NewActionUUID(), []*assets.GroupReference{doctors}, false),
					actions.NewAddContactGroups(hooks.NewActionUUID(), []*assets.GroupReference{testers}),
				},
			},
			SQLAssertions: []hooks.SQLAssertion{
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

	hooks.RunTestCases(t, tcs)
}
