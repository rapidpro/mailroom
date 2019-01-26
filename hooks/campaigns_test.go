package hooks

import (
	"testing"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/actions"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/testsuite"
)

func TestCampaigns(t *testing.T) {
	testsuite.Reset()

	doctors := assets.NewGroupReference(models.DoctorsGroupUUID, "Doctors")
	joined := assets.NewFieldReference("joined", "Joined")

	// init their values
	testsuite.DB().MustExec(
		`update contacts_contact set fields = fields - '8c1c1256-78d6-4a5b-9f1c-1761d5728251'
		WHERE id = $1`, models.CathyID)

	testsuite.DB().MustExec(
		`update contacts_contact set fields = fields ||
		'{"8c1c1256-78d6-4a5b-9f1c-1761d5728251": { "text": "2029-09-15T12:00:00+00:00", "datetime": "2029-09-15T12:00:00+00:00" }}'::jsonb
		WHERE id = $1`, models.BobID)

	tcs := []HookTestCase{
		HookTestCase{
			Actions: ContactActionMap{
				models.CathyID: []flows.Action{
					actions.NewAddContactGroupsAction(newActionUUID(), []*assets.GroupReference{doctors}),
					actions.NewSetContactFieldAction(newActionUUID(), joined, "2029-09-15T12:00:00+00:00"),
					actions.NewSetContactFieldAction(newActionUUID(), joined, ""),
				},
				models.BobID: []flows.Action{
					actions.NewAddContactGroupsAction(newActionUUID(), []*assets.GroupReference{doctors}),
					actions.NewSetContactFieldAction(newActionUUID(), joined, "2029-09-15T12:00:00+00:00"),
					actions.NewSetContactFieldAction(newActionUUID(), joined, "2029-09-15T12:00:00+00:00"),
				},
				models.GeorgeID: []flows.Action{
					actions.NewAddContactGroupsAction(newActionUUID(), []*assets.GroupReference{doctors}),
					actions.NewSetContactFieldAction(newActionUUID(), joined, "2029-09-15T12:00:00+00:00"),
					actions.NewRemoveContactGroupsAction(newActionUUID(), []*assets.GroupReference{doctors}, false),
				},
			},
			SQLAssertions: []SQLAssertion{
				SQLAssertion{
					SQL:   `select count(*) FROM campaigns_eventfire WHERE contact_id = $1`,
					Args:  []interface{}{models.CathyID},
					Count: 0,
				},
				SQLAssertion{
					SQL:   `select count(*) FROM campaigns_eventfire WHERE contact_id = $1`,
					Args:  []interface{}{models.BobID},
					Count: 2,
				},
				SQLAssertion{
					SQL:   `select count(*) FROM campaigns_eventfire WHERE contact_id = $1`,
					Args:  []interface{}{models.GeorgeID},
					Count: 0,
				},
			},
		},
	}

	RunActionTestCases(t, tcs)
}
