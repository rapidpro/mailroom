package hooks

import (
	"testing"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/actions"
	"github.com/nyaruka/goflow/utils/uuids"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/testsuite"
)

func TestCampaigns(t *testing.T) {
	testsuite.Reset()

	doctors := assets.NewGroupReference(models.DoctorsGroupUUID, "Doctors")
	joined := assets.NewFieldReference("joined", "Joined")

	// insert an event on our campaign that is based on created_on
	testsuite.DB().MustExec(
		`INSERT INTO campaigns_campaignevent(is_active, created_on, modified_on, uuid, "offset", unit, event_type, delivery_hour, 
											 campaign_id, created_by_id, modified_by_id, flow_id, relative_to_id, start_mode)
									   VALUES(TRUE, NOW(), NOW(), $1, 1000, 'W', 'F', -1, $2, 1, 1, $3, $4, 'I')`,
		uuids.New(), models.DoctorRemindersCampaignID, models.FavoritesFlowID, models.CreatedOnFieldID)

	// insert an event on our campaign that is based on last_seen_on
	testsuite.DB().MustExec(
		`INSERT INTO campaigns_campaignevent(is_active, created_on, modified_on, uuid, "offset", unit, event_type, delivery_hour, 
											 campaign_id, created_by_id, modified_by_id, flow_id, relative_to_id, start_mode)
									   VALUES(TRUE, NOW(), NOW(), $1, 2, 'D', 'F', -1, $2, 1, 1, $3, $4, 'I')`,
		uuids.New(), models.DoctorRemindersCampaignID, models.FavoritesFlowID, models.LastSeenOnFieldID)

	// init their values
	testsuite.DB().MustExec(
		`update contacts_contact set fields = fields - '8c1c1256-78d6-4a5b-9f1c-1761d5728251'
		WHERE id = $1`, models.CathyID)

	testsuite.DB().MustExec(
		`update contacts_contact set fields = fields ||
		'{"8c1c1256-78d6-4a5b-9f1c-1761d5728251": { "text": "2029-09-15T12:00:00+00:00", "datetime": "2029-09-15T12:00:00+00:00" }}'::jsonb
		WHERE id = $1`, models.BobID)

	tcs := []HookTestCase{
		{
			Msgs: ContactMsgMap{
				models.CathyID: flows.NewMsgIn(flows.MsgUUID(uuids.New()), models.CathyURN, nil, "Hi there", nil),
			},
			Actions: ContactActionMap{
				models.CathyID: []flows.Action{
					actions.NewRemoveContactGroups(newActionUUID(), []*assets.GroupReference{doctors}, false),
					actions.NewAddContactGroups(newActionUUID(), []*assets.GroupReference{doctors}),
					actions.NewSetContactField(newActionUUID(), joined, "2029-09-15T12:00:00+00:00"),
					actions.NewSetContactField(newActionUUID(), joined, ""),
				},
				models.BobID: []flows.Action{
					actions.NewAddContactGroups(newActionUUID(), []*assets.GroupReference{doctors}),
					actions.NewSetContactField(newActionUUID(), joined, "2029-09-15T12:00:00+00:00"),
					actions.NewSetContactField(newActionUUID(), joined, "2029-09-15T12:00:00+00:00"),
				},
				models.GeorgeID: []flows.Action{
					actions.NewAddContactGroups(newActionUUID(), []*assets.GroupReference{doctors}),
					actions.NewSetContactField(newActionUUID(), joined, "2029-09-15T12:00:00+00:00"),
					actions.NewRemoveContactGroups(newActionUUID(), []*assets.GroupReference{doctors}, false),
				},
			},
			SQLAssertions: []SQLAssertion{
				{
					SQL:   `select count(*) FROM campaigns_eventfire WHERE contact_id = $1`,
					Args:  []interface{}{models.CathyID},
					Count: 2,
				},
				{
					SQL:   `select count(*) FROM campaigns_eventfire WHERE contact_id = $1`,
					Args:  []interface{}{models.BobID},
					Count: 3,
				},
				{
					SQL:   `select count(*) FROM campaigns_eventfire WHERE contact_id = $1`,
					Args:  []interface{}{models.GeorgeID},
					Count: 0,
				},
			},
		},
	}

	RunHookTestCases(t, tcs)
}
