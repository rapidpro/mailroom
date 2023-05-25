package campaigns_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/goflow/flows/triggers"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/queue"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/core/tasks/campaigns"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/nyaruka/redisx"
	"github.com/nyaruka/redisx/assertredis"
	"github.com/stretchr/testify/assert"
)

func TestFireCampaignEvents(t *testing.T) {
	_, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetAll)

	rc := rt.RP.Get()
	defer rc.Close()

	marker := redisx.NewIntervalSet("campaign_event", time.Hour*24, 2)
	campaign := triggers.NewCampaignReference(triggers.CampaignUUID(testdata.RemindersCampaign.UUID), "Doctor Reminders")

	// create event fires for event #3 (Pick A Number, start mode SKIP)
	now := time.Now()
	fire1ID := testdata.InsertEventFire(rt, testdata.Cathy, testdata.RemindersEvent3, now)
	fire2ID := testdata.InsertEventFire(rt, testdata.Bob, testdata.RemindersEvent3, now)
	fire3ID := testdata.InsertEventFire(rt, testdata.Alexandria, testdata.RemindersEvent3, now)

	// create waiting sessions for Cathy and Alexandria
	testdata.InsertWaitingSession(rt, testdata.Org1, testdata.Cathy, models.FlowTypeVoice, testdata.IVRFlow, models.NilCallID, time.Now(), time.Now(), false, nil)
	testdata.InsertWaitingSession(rt, testdata.Org1, testdata.Alexandria, models.FlowTypeMessaging, testdata.Favorites, models.NilCallID, time.Now(), time.Now(), false, nil)

	// simulates the cron job picking up pending fires, marking them in redis, and queueing a task to handle them
	fireFires := func(fireIDs []models.FireID, flow *testdata.Flow, ce *testdata.CampaignEvent) {
		for _, fid := range fireIDs {
			marker.Add(rc, fmt.Sprintf("%d", fid))
		}

		task := &campaigns.FireCampaignEventTask{
			FireIDs:      fireIDs,
			EventID:      int64(ce.ID),
			EventUUID:    string(ce.UUID),
			FlowUUID:     flow.UUID,
			CampaignUUID: string(campaign.UUID),
			CampaignName: campaign.Name,
		}

		err := tasks.Queue(rc, queue.BatchQueue, testdata.Org1.ID, task, queue.DefaultPriority)
		assert.NoError(t, err)

		testsuite.FlushTasks(t, rt)

		// and left in redis marker
		for _, fid := range fireIDs {
			assertredis.SIsMember(t, rt.RP, fmt.Sprintf("campaign_event:%s", time.Now().Format("2006-01-02")), fmt.Sprintf("%d", fid), true)
		}
	}

	fireFires([]models.FireID{fire1ID, fire2ID, fire3ID}, testdata.PickANumber, testdata.RemindersEvent3)

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
	fire4ID := testdata.InsertEventFire(rt, testdata.Cathy, testdata.RemindersEvent2, now)
	fire5ID := testdata.InsertEventFire(rt, testdata.Bob, testdata.RemindersEvent2, now)
	fire6ID := testdata.InsertEventFire(rt, testdata.Alexandria, testdata.RemindersEvent2, now)

	fireFires([]models.FireID{fire4ID, fire5ID, fire6ID}, testdata.CampaignFlow, testdata.RemindersEvent2)

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
	fire7ID := testdata.InsertEventFire(rt, testdata.Cathy, testdata.RemindersEvent1, now)
	fire8ID := testdata.InsertEventFire(rt, testdata.Bob, testdata.RemindersEvent1, now)
	fire9ID := testdata.InsertEventFire(rt, testdata.Alexandria, testdata.RemindersEvent1, now)

	fireFires([]models.FireID{fire7ID, fire8ID, fire9ID}, testdata.Favorites, testdata.RemindersEvent1)

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

	// test handling fires for a deleted campaign event
	rt.DB.MustExec(`UPDATE campaigns_campaignevent SET is_active = FALSE WHERE id = $1`, testdata.RemindersEvent1.ID)
	models.FlushCache()

	fire10ID := testdata.InsertEventFire(rt, testdata.Cathy, testdata.RemindersEvent1, now)

	fireFires([]models.FireID{fire10ID}, testdata.Favorites, testdata.RemindersEvent1)

	// event fire should be deleted
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM campaigns_eventfire WHERE id = $1`, fire10ID).Returns(0)

	// test handling fires for a deleted flow
	rt.DB.MustExec(`UPDATE flows_flow SET is_active = FALSE WHERE id = $1`, testdata.PickANumber.ID)
	models.FlushCache()

	fire11ID := testdata.InsertEventFire(rt, testdata.Cathy, testdata.RemindersEvent3, now)

	fireFires([]models.FireID{fire11ID}, testdata.PickANumber, testdata.RemindersEvent3)

	// event fire should be deleted
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM campaigns_eventfire WHERE id = $1`, fire11ID).Returns(0)
}
