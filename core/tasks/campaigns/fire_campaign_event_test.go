package campaigns_test

import (
	"testing"
	"time"

	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/goflow/flows/triggers"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/tasks/campaigns"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/assert"
)

func TestFireCampaignEvents(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetData | testsuite.ResetRedis)

	campaign := triggers.NewCampaignReference(triggers.CampaignUUID(testdata.RemindersCampaign.UUID), "Doctor Reminders")

	// create event fires for event #3 (Pick A Number, start mode SKIP)
	now := time.Now()
	fire1ID := testdata.InsertEventFire(rt.DB, testdata.Cathy, testdata.RemindersEvent3, now)
	fire2ID := testdata.InsertEventFire(rt.DB, testdata.Bob, testdata.RemindersEvent3, now)
	fire3ID := testdata.InsertEventFire(rt.DB, testdata.Alexandria, testdata.RemindersEvent3, now)

	// create waiting sessions for Cathy and Alexandria
	testdata.InsertWaitingSession(rt.DB, testdata.Org1, testdata.Cathy, models.FlowTypeVoice, testdata.IVRFlow, models.NilCallID, time.Now(), time.Now(), false, nil)
	testdata.InsertWaitingSession(rt.DB, testdata.Org1, testdata.Alexandria, models.FlowTypeMessaging, testdata.Favorites, models.NilCallID, time.Now(), time.Now(), false, nil)

	fires := []*models.EventFire{
		{
			FireID:    fire1ID,
			EventID:   testdata.RemindersEvent3.ID,
			ContactID: testdata.Cathy.ID,
			Scheduled: now,
		},
		{
			FireID:    fire2ID,
			EventID:   testdata.RemindersEvent3.ID,
			ContactID: testdata.Bob.ID,
			Scheduled: now,
		},
		{
			FireID:    fire3ID,
			EventID:   testdata.RemindersEvent3.ID,
			ContactID: testdata.Alexandria.ID,
			Scheduled: now,
		},
	}
	startedIDs, err := campaigns.FireCampaignEvents(ctx, rt, testdata.Org1.ID, fires, testdata.PickANumber.UUID, campaign, triggers.CampaignEventUUID(testdata.RemindersEvent3.UUID))
	assert.NoError(t, err)
	assert.ElementsMatch(t, []models.ContactID{testdata.Bob.ID}, startedIDs)

	// cathy has her existing waiting session because event skipped her
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM flows_flowsession WHERE contact_id = $1 AND status = 'W'`, testdata.Cathy.ID).Returns(1)
	assertdb.Query(t, rt.DB, `SELECT current_flow_id FROM flows_flowsession WHERE contact_id = $1 AND status = 'W'`, testdata.Cathy.ID).Returns(int64(testdata.IVRFlow.ID))
	assertdb.Query(t, rt.DB, `SELECT fired_result from campaigns_eventfire WHERE contact_id = $1 AND event_id = $2`, testdata.Cathy.ID, testdata.RemindersEvent3.ID).Returns("S")

	// bob's waiting session is the campaign event because he didn't have a waiting session
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM flows_flowsession WHERE contact_id = $1 AND status = 'W'`, testdata.Bob.ID).Returns(1)
	assertdb.Query(t, rt.DB, `SELECT current_flow_id FROM flows_flowsession WHERE contact_id = $1 AND status = 'W'`, testdata.Bob.ID).Returns(int64(testdata.PickANumber.ID))
	assertdb.Query(t, rt.DB, `SELECT fired_result from campaigns_eventfire WHERE contact_id = $1 AND event_id = $2`, testdata.Bob.ID, testdata.RemindersEvent3.ID).Returns("F")

	// alexandria has her existing waiting session because event skipped her
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM flows_flowsession WHERE contact_id = $1 AND status = 'W'`, testdata.Alexandria.ID).Returns(1)
	assertdb.Query(t, rt.DB, `SELECT current_flow_id FROM flows_flowsession WHERE contact_id = $1 AND status = 'W'`, testdata.Alexandria.ID).Returns(int64(testdata.Favorites.ID))
	assertdb.Query(t, rt.DB, `SELECT fired_result from campaigns_eventfire WHERE contact_id = $1 AND event_id = $2`, testdata.Alexandria.ID, testdata.RemindersEvent3.ID).Returns("S")

	// all event fires fired
	assertdb.Query(t, rt.DB, `SELECT count(*) from campaigns_eventfire WHERE fired IS NULL`).Returns(0)

	// create event fires for event #2 (message, start mode PASSIVE)
	now = time.Now()
	fire4ID := testdata.InsertEventFire(rt.DB, testdata.Cathy, testdata.RemindersEvent2, now)
	fire5ID := testdata.InsertEventFire(rt.DB, testdata.Bob, testdata.RemindersEvent2, now)
	fire6ID := testdata.InsertEventFire(rt.DB, testdata.Alexandria, testdata.RemindersEvent2, now)

	fires = []*models.EventFire{
		{
			FireID:    fire4ID,
			EventID:   testdata.RemindersEvent2.ID,
			ContactID: testdata.Cathy.ID,
			Scheduled: now,
		},
		{
			FireID:    fire5ID,
			EventID:   testdata.RemindersEvent2.ID,
			ContactID: testdata.Bob.ID,
			Scheduled: now,
		},
		{
			FireID:    fire6ID,
			EventID:   testdata.RemindersEvent2.ID,
			ContactID: testdata.Alexandria.ID,
			Scheduled: now,
		},
	}

	startedIDs, err = campaigns.FireCampaignEvents(ctx, rt, testdata.Org1.ID, fires, testdata.CampaignFlow.UUID, campaign, triggers.CampaignEventUUID(testdata.RemindersEvent2.UUID))
	assert.NoError(t, err)
	assert.ElementsMatch(t, []models.ContactID{testdata.Cathy.ID, testdata.Bob.ID, testdata.Alexandria.ID}, startedIDs)

	// cathy still has her existing waiting session and now a completed one
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM flows_flowsession WHERE contact_id = $1 AND status = 'W'`, testdata.Cathy.ID).Returns(1)
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM flows_flowsession WHERE contact_id = $1 AND status = 'C'`, testdata.Cathy.ID).Returns(1)
	assertdb.Query(t, rt.DB, `SELECT fired_result from campaigns_eventfire WHERE contact_id = $1 AND event_id = $2`, testdata.Cathy.ID, testdata.RemindersEvent2.ID).Returns("F")

	// bob still has one waiting session from the previous campaign event and now a completed one
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM flows_flowsession WHERE contact_id = $1 AND status = 'W'`, testdata.Bob.ID).Returns(1)
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM flows_flowsession WHERE contact_id = $1 AND status = 'C'`, testdata.Bob.ID).Returns(1)
	assertdb.Query(t, rt.DB, `SELECT fired_result from campaigns_eventfire WHERE contact_id = $1 AND event_id = $2`, testdata.Bob.ID, testdata.RemindersEvent2.ID).Returns("F")

	// alexandria still has her existing waiting session and now a completed one
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM flows_flowsession WHERE contact_id = $1 AND status = 'W'`, testdata.Alexandria.ID).Returns(1)
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM flows_flowsession WHERE contact_id = $1 AND status = 'C'`, testdata.Alexandria.ID).Returns(1)
	assertdb.Query(t, rt.DB, `SELECT fired_result from campaigns_eventfire WHERE contact_id = $1 AND event_id = $2`, testdata.Alexandria.ID, testdata.RemindersEvent2.ID).Returns("F")

	// create event fires for event #1 (flow, start mode INTERRUPT)
	now = time.Now()
	fire7ID := testdata.InsertEventFire(rt.DB, testdata.Cathy, testdata.RemindersEvent1, now)
	fire8ID := testdata.InsertEventFire(rt.DB, testdata.Bob, testdata.RemindersEvent1, now)
	fire9ID := testdata.InsertEventFire(rt.DB, testdata.Alexandria, testdata.RemindersEvent1, now)

	fires = []*models.EventFire{
		{
			FireID:    fire7ID,
			EventID:   testdata.RemindersEvent1.ID,
			ContactID: testdata.Cathy.ID,
			Scheduled: now,
		},
		{
			FireID:    fire8ID,
			EventID:   testdata.RemindersEvent1.ID,
			ContactID: testdata.Bob.ID,
			Scheduled: now,
		},
		{
			FireID:    fire9ID,
			EventID:   testdata.RemindersEvent1.ID,
			ContactID: testdata.Alexandria.ID,
			Scheduled: now,
		},
	}

	startedIDs, err = campaigns.FireCampaignEvents(ctx, rt, testdata.Org1.ID, fires, testdata.Favorites.UUID, campaign, triggers.CampaignEventUUID(testdata.RemindersEvent1.UUID))
	assert.NoError(t, err)
	assert.ElementsMatch(t, []models.ContactID{testdata.Cathy.ID, testdata.Bob.ID, testdata.Alexandria.ID}, startedIDs)

	// cathy's existing waiting session should now be interrupted and now she has a waiting session in the Favorites flow
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM flows_flowsession WHERE contact_id = $1 AND status = 'I'`, testdata.Cathy.ID).Returns(1)
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM flows_flowsession WHERE contact_id = $1 AND status = 'C'`, testdata.Cathy.ID).Returns(1)
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM flows_flowsession WHERE contact_id = $1 AND status = 'W' AND current_flow_id = $2`, testdata.Cathy.ID, testdata.Favorites.ID).Returns(1)
	assertdb.Query(t, rt.DB, `SELECT fired_result from campaigns_eventfire WHERE contact_id = $1 AND event_id = $2`, testdata.Cathy.ID, testdata.RemindersEvent1.ID).Returns("F")

	// bob's session from the first campaign event should now be interrupted and he has a new waiting session
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM flows_flowsession WHERE contact_id = $1 AND status = 'I'`, testdata.Bob.ID).Returns(1)
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM flows_flowsession WHERE contact_id = $1 AND status = 'C'`, testdata.Bob.ID).Returns(1)
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM flows_flowsession WHERE contact_id = $1 AND status = 'W' AND current_flow_id = $2`, testdata.Bob.ID, testdata.Favorites.ID).Returns(1)
	assertdb.Query(t, rt.DB, `SELECT fired_result from campaigns_eventfire WHERE contact_id = $1 AND event_id = $2`, testdata.Bob.ID, testdata.RemindersEvent1.ID).Returns("F")

	// alexandria's existing waiting session should now be interrupted and now she has a waiting session in the Favorites flow
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM flows_flowsession WHERE contact_id = $1 AND status = 'I'`, testdata.Alexandria.ID).Returns(1)
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM flows_flowsession WHERE contact_id = $1 AND status = 'C'`, testdata.Alexandria.ID).Returns(1)
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM flows_flowsession WHERE contact_id = $1 AND status = 'W' AND current_flow_id = $2`, testdata.Alexandria.ID, testdata.Favorites.ID).Returns(1)
	assertdb.Query(t, rt.DB, `SELECT fired_result from campaigns_eventfire WHERE contact_id = $1 AND event_id = $2`, testdata.Alexandria.ID, testdata.RemindersEvent1.ID).Returns("F")
}
