package hooks

import (
	"testing"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/actions"
	"github.com/nyaruka/mailroom/testsuite"
)

func TestCampaigns(t *testing.T) {
	testsuite.Reset()

	doctors := assets.NewGroupReference(assets.GroupUUID("85a5a793-4741-4896-b55e-05af65f3c0fa"), "Doctors")
	joined := assets.NewFieldReference("joined", "Joined")

	// init their values
	testsuite.DB().MustExec(
		`update contacts_contact set fields = fields - '8c1c1256-78d6-4a5b-9f1c-1761d5728251'
		WHERE id = $1`, Cathy)

	testsuite.DB().MustExec(
		`update contacts_contact set fields = fields ||
		'{"8c1c1256-78d6-4a5b-9f1c-1761d5728251": { "text": "2029-09-15T12:00:00+00:00", "datetime": "2029-09-15T12:00:00+00:00" }}'::jsonb
		WHERE id = $1`, Bob)

	tcs := []HookTestCase{
		HookTestCase{
			Actions: ContactActionMap{
				Cathy: []flows.Action{
					actions.NewAddContactGroupsAction(newActionUUID(), []*assets.GroupReference{doctors}),
					actions.NewSetContactFieldAction(newActionUUID(), joined, "2029-09-15T12:00:00+00:00"),
					actions.NewSetContactFieldAction(newActionUUID(), joined, ""),
				},
				Bob: []flows.Action{
					actions.NewAddContactGroupsAction(newActionUUID(), []*assets.GroupReference{doctors}),
					actions.NewSetContactFieldAction(newActionUUID(), joined, "2029-09-15T12:00:00+00:00"),
					actions.NewSetContactFieldAction(newActionUUID(), joined, "2029-09-15T12:00:00+00:00"),
				},
				Evan: []flows.Action{
					actions.NewAddContactGroupsAction(newActionUUID(), []*assets.GroupReference{doctors}),
					actions.NewSetContactFieldAction(newActionUUID(), joined, "2029-09-15T12:00:00+00:00"),
					actions.NewRemoveContactGroupsAction(newActionUUID(), []*assets.GroupReference{doctors}, false),
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

	RunActionTestCases(t, tcs)
}
