package models_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/testsuite"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCampaignSchedule(t *testing.T) {
	eastern, _ := time.LoadLocation("US/Eastern")
	nilDate := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)

	tcs := []struct {
		Offset       int
		Unit         models.OffsetUnit
		DeliveryHour int

		Timezone *time.Location
		Now      time.Time
		Start    time.Time

		HasError  bool
		Scheduled time.Time
		Delta     time.Duration
	}{
		// this crosses a DST boundary, so two days is really 49 hours (fall back)
		{2, models.OffsetDay, models.NilDeliveryHour, eastern, time.Now(), time.Date(2029, 11, 3, 0, 30, 0, 0, eastern),
			false, time.Date(2029, 11, 5, 0, 30, 0, 0, eastern), time.Hour * 49},

		// this also crosses a boundary but in the other direction
		{2, models.OffsetDay, models.NilDeliveryHour, eastern, time.Now(), time.Date(2029, 3, 10, 2, 30, 0, 0, eastern),
			false, time.Date(2029, 3, 12, 2, 30, 0, 0, eastern), time.Hour * 47},

		// this event is in the past, no schedule
		{2, models.OffsetDay, models.NilDeliveryHour, eastern, time.Date(2018, 10, 31, 0, 0, 0, 0, eastern), time.Date(2018, 10, 15, 0, 0, 0, 0, eastern),
			false, nilDate, 0},

		{2, models.OffsetMinute, models.NilDeliveryHour, eastern, time.Now(), time.Date(2029, 1, 1, 2, 58, 0, 0, eastern),
			false, time.Date(2029, 1, 1, 3, 0, 0, 0, eastern), time.Minute * 2},

		{2, models.OffsetMinute, models.NilDeliveryHour, eastern, time.Now(), time.Date(2029, 1, 1, 2, 57, 32, 0, eastern),
			false, time.Date(2029, 1, 1, 3, 0, 0, 0, eastern), time.Minute*2 + time.Second*28},

		{-2, models.OffsetHour, models.NilDeliveryHour, eastern, time.Now(), time.Date(2029, 1, 2, 1, 58, 0, 0, eastern),
			false, time.Date(2029, 1, 1, 23, 58, 0, 0, eastern), time.Hour * -2},

		{2, models.OffsetWeek, models.NilDeliveryHour, eastern, time.Now(), time.Date(2029, 1, 20, 1, 58, 0, 0, eastern),
			false, time.Date(2029, 2, 3, 1, 58, 0, 0, eastern), time.Hour * 24 * 14},

		{2, models.OffsetWeek, 14, eastern, time.Now(), time.Date(2029, 1, 20, 1, 58, 0, 0, eastern),
			false, time.Date(2029, 2, 3, 14, 0, 0, 0, eastern), time.Hour*24*14 + 13*time.Hour - 58*time.Minute},

		{2, "L", 14, eastern, time.Now(), time.Date(2029, 1, 20, 1, 58, 0, 0, eastern),
			true, nilDate, 0},
	}

	for i, tc := range tcs {
		evtJSON := fmt.Sprintf(`{"offset": %d, "unit": "%s", "delivery_hour": %d}`, tc.Offset, tc.Unit, tc.DeliveryHour)
		evt := &models.CampaignEvent{}
		err := json.Unmarshal([]byte(evtJSON), evt)
		require.NoError(t, err)

		scheduled, err := evt.ScheduleForTime(tc.Timezone, tc.Now, tc.Start)

		if err != nil {
			assert.True(t, tc.HasError, "%d: received unexpected error %s", i, err.Error())
		} else if tc.Scheduled.IsZero() {
			assert.Nil(t, scheduled, "%d: received unexpected value", i)
		} else {
			assert.Equal(t, tc.Scheduled.In(time.UTC), scheduled.In(time.UTC), "%d: mismatch in expected scheduled and actual", i)
			assert.Equal(t, scheduled.Sub(tc.Start), tc.Delta, "%d: mismatch in expected delta", i)
		}
	}
}

func TestAddEventFires(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()
	testsuite.Reset()

	scheduled1 := time.Date(2020, 9, 8, 14, 38, 30, 123456789, time.UTC)

	err := models.AddEventFires(ctx, db, []*models.FireAdd{
		{ContactID: models.CathyID, EventID: models.RemindersEvent1ID, Scheduled: scheduled1},
		{ContactID: models.BobID, EventID: models.RemindersEvent1ID, Scheduled: scheduled1},
		{ContactID: models.BobID, EventID: models.RemindersEvent2ID, Scheduled: scheduled1},
	})
	require.NoError(t, err)

	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM campaigns_eventfire`, nil, 3)
	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM campaigns_eventfire WHERE contact_id = $1 AND event_id = $2`, []interface{}{models.CathyID, models.RemindersEvent1ID}, 1)
	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM campaigns_eventfire WHERE contact_id = $1 AND event_id = $2`, []interface{}{models.BobID, models.RemindersEvent1ID}, 1)
	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM campaigns_eventfire WHERE contact_id = $1 AND event_id = $2`, []interface{}{models.BobID, models.RemindersEvent2ID}, 1)

	db.MustExec(`UPDATE campaigns_eventfire SET fired = NOW() WHERE contact_id = $1`, models.CathyID)

	scheduled2 := time.Date(2020, 9, 8, 14, 38, 30, 123456789, time.UTC)

	err = models.AddEventFires(ctx, db, []*models.FireAdd{
		{ContactID: models.CathyID, EventID: models.RemindersEvent1ID, Scheduled: scheduled2}, // fine because previous one now has non-null fired
		{ContactID: models.BobID, EventID: models.RemindersEvent1ID, Scheduled: scheduled2},   // won't be added due to conflict
	})
	require.NoError(t, err)

	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM campaigns_eventfire`, nil, 4)
	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM campaigns_eventfire WHERE contact_id = $1 AND event_id = $2`, []interface{}{models.CathyID, models.RemindersEvent1ID}, 2)
	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM campaigns_eventfire WHERE contact_id = $1`, []interface{}{models.BobID}, 2)
}
