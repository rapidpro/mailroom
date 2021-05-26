package handlers_test

import (
	"testing"

	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/actions"
	"github.com/nyaruka/mailroom/core/handlers"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
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
		uuids.New(), models.DoctorRemindersCampaignID, testdata.Favorites.ID, models.CreatedOnFieldID)

	// insert an event on our campaign that is based on last_seen_on
	testsuite.DB().MustExec(
		`INSERT INTO campaigns_campaignevent(is_active, created_on, modified_on, uuid, "offset", unit, event_type, delivery_hour, 
											 campaign_id, created_by_id, modified_by_id, flow_id, relative_to_id, start_mode)
									   VALUES(TRUE, NOW(), NOW(), $1, 2, 'D', 'F', -1, $2, 1, 1, $3, $4, 'I')`,
		uuids.New(), models.DoctorRemindersCampaignID, testdata.Favorites.ID, models.LastSeenOnFieldID)

	// init their values
	testsuite.DB().MustExec(
		`update contacts_contact set fields = fields - '8c1c1256-78d6-4a5b-9f1c-1761d5728251'
		WHERE id = $1`, testdata.Cathy.ID)

	testsuite.DB().MustExec(
		`update contacts_contact set fields = fields ||
		'{"8c1c1256-78d6-4a5b-9f1c-1761d5728251": { "text": "2029-09-15T12:00:00+00:00", "datetime": "2029-09-15T12:00:00+00:00" }}'::jsonb
		WHERE id = $1`, testdata.Bob.ID)

	tcs := []handlers.TestCase{
		{
			Msgs: handlers.ContactMsgMap{
				testdata.Cathy.ID: flows.NewMsgIn(flows.MsgUUID(uuids.New()), testdata.Cathy.URN, nil, "Hi there", nil),
			},
			Actions: handlers.ContactActionMap{
				testdata.Cathy.ID: []flows.Action{
					actions.NewRemoveContactGroups(handlers.NewActionUUID(), []*assets.GroupReference{doctors}, false),
					actions.NewAddContactGroups(handlers.NewActionUUID(), []*assets.GroupReference{doctors}),
					actions.NewSetContactField(handlers.NewActionUUID(), joined, "2029-09-15T12:00:00+00:00"),
					actions.NewSetContactField(handlers.NewActionUUID(), joined, ""),
				},
				testdata.Bob.ID: []flows.Action{
					actions.NewAddContactGroups(handlers.NewActionUUID(), []*assets.GroupReference{doctors}),
					actions.NewSetContactField(handlers.NewActionUUID(), joined, "2029-09-15T12:00:00+00:00"),
					actions.NewSetContactField(handlers.NewActionUUID(), joined, "2029-09-15T12:00:00+00:00"),
				},
				testdata.George.ID: []flows.Action{
					actions.NewAddContactGroups(handlers.NewActionUUID(), []*assets.GroupReference{doctors}),
					actions.NewSetContactField(handlers.NewActionUUID(), joined, "2029-09-15T12:00:00+00:00"),
					actions.NewRemoveContactGroups(handlers.NewActionUUID(), []*assets.GroupReference{doctors}, false),
				},
			},
			SQLAssertions: []handlers.SQLAssertion{
				{
					SQL:   `select count(*) FROM campaigns_eventfire WHERE contact_id = $1`,
					Args:  []interface{}{testdata.Cathy.ID},
					Count: 2,
				},
				{
					SQL:   `select count(*) FROM campaigns_eventfire WHERE contact_id = $1`,
					Args:  []interface{}{testdata.Bob.ID},
					Count: 3,
				},
				{
					SQL:   `select count(*) FROM campaigns_eventfire WHERE contact_id = $1`,
					Args:  []interface{}{testdata.George.ID},
					Count: 0,
				},
			},
		},
	}

	handlers.RunTestCases(t, tcs)
}
