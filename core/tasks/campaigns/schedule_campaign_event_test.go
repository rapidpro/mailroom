package campaigns_test

import (
	"testing"
	"time"

	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/config"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/tasks/campaigns"
	"github.com/nyaruka/mailroom/testsuite"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScheduleCampaignEvent(t *testing.T) {
	testsuite.Reset()
	ctx := testsuite.CTX()
	db := testsuite.DB()
	mr := &mailroom.Mailroom{Config: config.Mailroom, DB: db, RP: testsuite.RP(), ElasticClient: nil}

	models.FlushCache()

	// add bob, george and alexandria to doctors group which campaign is based on
	db.MustExec(`INSERT INTO contacts_contactgroup_contacts(contact_id, contactgroup_id) VALUES($1, $2)`, models.BobID, models.DoctorsGroupID)
	db.MustExec(`INSERT INTO contacts_contactgroup_contacts(contact_id, contactgroup_id) VALUES($1, $2)`, models.GeorgeID, models.DoctorsGroupID)
	db.MustExec(`INSERT INTO contacts_contactgroup_contacts(contact_id, contactgroup_id) VALUES($1, $2)`, models.AlexandriaID, models.DoctorsGroupID)

	// give bob and george values for joined in the future
	db.MustExec(`UPDATE contacts_contact SET fields = '{"d83aae24-4bbf-49d0-ab85-6bfd201eac6d": {"datetime": "2030-01-01T00:00:00Z"}}' WHERE id = $1`, models.BobID)
	db.MustExec(`UPDATE contacts_contact SET fields = '{"d83aae24-4bbf-49d0-ab85-6bfd201eac6d": {"datetime": "2030-08-18T11:31:30Z"}}' WHERE id = $1`, models.GeorgeID)

	// give alexandria a value in the past
	db.MustExec(`UPDATE contacts_contact SET fields = '{"d83aae24-4bbf-49d0-ab85-6bfd201eac6d": {"datetime": "2015-01-01T00:00:00Z"}}' WHERE id = $1`, models.AlexandriaID)

	db.MustExec(`DELETE FROM campaigns_eventfire`)

	// campaign has two events configured on the joined field
	//  1. +5 Days (12:00) start favorites flow
	//  2. +10 Minutes send message

	// schedule first event...
	task := &campaigns.ScheduleCampaignEventTask{CampaignEventID: models.RemindersEvent1ID}
	err := task.Perform(ctx, mr, models.Org1)
	require.NoError(t, err)

	// cathy has no value for joined and alexandia has a value too far in past, but bob and george will have values...
	assertContactFires(t, models.RemindersEvent1ID, map[models.ContactID]time.Time{
		models.BobID:    time.Date(2030, 1, 5, 20, 0, 0, 0, time.UTC),  // 12:00 in PST
		models.GeorgeID: time.Date(2030, 8, 23, 19, 0, 0, 0, time.UTC), // 12:00 in PST with DST
	})

	// schedule second event...
	task = &campaigns.ScheduleCampaignEventTask{CampaignEventID: models.RemindersEvent2ID}
	err = task.Perform(ctx, mr, models.Org1)
	require.NoError(t, err)

	assertContactFires(t, models.RemindersEvent2ID, map[models.ContactID]time.Time{
		models.BobID:    time.Date(2030, 1, 1, 0, 10, 0, 0, time.UTC),
		models.GeorgeID: time.Date(2030, 8, 18, 11, 42, 0, 0, time.UTC),
	})

	// fires for first event unaffected
	assertContactFires(t, models.RemindersEvent1ID, map[models.ContactID]time.Time{
		models.BobID:    time.Date(2030, 1, 5, 20, 0, 0, 0, time.UTC),
		models.GeorgeID: time.Date(2030, 8, 23, 19, 0, 0, 0, time.UTC),
	})

	// remove alexandria from campaign group
	db.MustExec(`DELETE FROM contacts_contactgroup_contacts WHERE contact_id = $1`, models.AlexandriaID)

	// bump created_on for cathy and alexandria
	db.MustExec(`UPDATE contacts_contact SET created_on = '2035-01-01T00:00:00Z' WHERE id = $1 OR id = $2`, models.CathyID, models.AlexandriaID)

	// create new campaign event based on created_on + 5 minutes
	event3 := insertCampaignEvent(t, models.DoctorRemindersCampaignID, models.FavoritesFlowID, models.CreatedOnFieldID, 5, "M")

	task = &campaigns.ScheduleCampaignEventTask{CampaignEventID: event3}
	err = task.Perform(ctx, mr, models.Org1)
	require.NoError(t, err)

	// only cathy is in the group and new enough to have a fire
	assertContactFires(t, event3, map[models.ContactID]time.Time{
		models.CathyID: time.Date(2035, 1, 1, 0, 5, 0, 0, time.UTC),
	})

	// create new campaign event based on last_seen_on + 1 day
	event4 := insertCampaignEvent(t, models.DoctorRemindersCampaignID, models.FavoritesFlowID, models.LastSeenOnFieldID, 1, "D")

	// bump last_seen_on for bob
	db.MustExec(`UPDATE contacts_contact SET last_seen_on = '2040-01-01T00:00:00Z' WHERE id = $1`, models.BobID)

	task = &campaigns.ScheduleCampaignEventTask{CampaignEventID: event4}
	err = task.Perform(ctx, mr, models.Org1)
	require.NoError(t, err)

	assertContactFires(t, event4, map[models.ContactID]time.Time{
		models.BobID: time.Date(2040, 1, 2, 0, 0, 0, 0, time.UTC),
	})
}

func insertCampaignEvent(t *testing.T, campaignID models.CampaignID, flowID models.FlowID, relativeToID models.FieldID, offset int, unit string) models.CampaignEventID {
	var eventID models.CampaignEventID
	err := testsuite.DB().Get(&eventID, `
	INSERT INTO campaigns_campaignevent(is_active, created_on, modified_on, uuid, "offset", unit, event_type, delivery_hour, campaign_id, created_by_id, modified_by_id, flow_id, relative_to_id, start_mode)
	VALUES(TRUE, NOW(), NOW(), $1, $5, $6, 'F', -1, $2, 1, 1, $3, $4, 'I') RETURNING id`, uuids.New(), campaignID, flowID, relativeToID, offset, unit)
	require.NoError(t, err)

	return eventID
}

func assertContactFires(t *testing.T, eventID models.CampaignEventID, expected map[models.ContactID]time.Time) {
	type idAndTime struct {
		ContactID models.ContactID `db:"contact_id"`
		Scheduled time.Time        `db:"scheduled"`
	}

	actualAsSlice := make([]idAndTime, 0)
	err := testsuite.DB().Select(&actualAsSlice, `SELECT contact_id, scheduled FROM campaigns_eventfire WHERE event_id = $1`, eventID)
	require.NoError(t, err)

	actual := make(map[models.ContactID]time.Time)
	for _, it := range actualAsSlice {
		actual[it.ContactID] = it.Scheduled
	}

	assert.Equal(t, expected, actual)
}
