package models

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCampaignSchedule(t *testing.T) {
	eastern, _ := time.LoadLocation("US/Eastern")
	nilDate := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)

	tcs := []struct {
		Offset       int
		Unit         OffsetUnit
		DeliveryHour int

		Timezone *time.Location
		Now      time.Time
		Start    time.Time

		HasError  bool
		Scheduled time.Time
		Delta     time.Duration
	}{
		// this crosses a DST boundary, so two days is really 49 hours (fall back)
		{2, OffsetDay, NilDeliveryHour, eastern, time.Now(), time.Date(2029, 11, 3, 0, 30, 0, 0, eastern),
			false, time.Date(2029, 11, 5, 0, 30, 0, 0, eastern), time.Hour * 49},

		// this also crosses a boundary but in the other direction
		{2, OffsetDay, NilDeliveryHour, eastern, time.Now(), time.Date(2029, 3, 10, 2, 30, 0, 0, eastern),
			false, time.Date(2029, 3, 12, 2, 30, 0, 0, eastern), time.Hour * 47},

		// this event is in the past, no schedule
		{2, OffsetDay, NilDeliveryHour, eastern, time.Date(2018, 10, 31, 0, 0, 0, 0, eastern), time.Date(2018, 10, 15, 0, 0, 0, 0, eastern),
			false, nilDate, 0},

		{2, OffsetMinute, NilDeliveryHour, eastern, time.Now(), time.Date(2029, 1, 1, 2, 58, 0, 0, eastern),
			false, time.Date(2029, 1, 1, 3, 0, 0, 0, eastern), time.Minute * 2},

		{2, OffsetMinute, NilDeliveryHour, eastern, time.Now(), time.Date(2029, 1, 1, 2, 57, 32, 35, eastern),
			false, time.Date(2029, 1, 1, 3, 0, 0, 0, eastern), time.Minute * 2},

		{-2, OffsetHour, NilDeliveryHour, eastern, time.Now(), time.Date(2029, 1, 2, 1, 58, 0, 0, eastern),
			false, time.Date(2029, 1, 1, 23, 58, 0, 0, eastern), time.Hour * -2},

		{2, OffsetWeek, NilDeliveryHour, eastern, time.Now(), time.Date(2029, 1, 20, 1, 58, 0, 0, eastern),
			false, time.Date(2029, 2, 3, 1, 58, 0, 0, eastern), time.Hour * 24 * 14},

		{2, OffsetWeek, 14, eastern, time.Now(), time.Date(2029, 1, 20, 1, 58, 0, 0, eastern),
			false, time.Date(2029, 2, 3, 14, 0, 0, 0, eastern), time.Hour*24*14 + 13*time.Hour - 58*time.Minute},

		{2, "L", 14, eastern, time.Now(), time.Date(2029, 1, 20, 1, 58, 0, 0, eastern),
			true, nilDate, 0},
	}

	for i, tc := range tcs {
		evt := &CampaignEvent{}
		evt.e.Offset = tc.Offset
		evt.e.Unit = tc.Unit
		evt.e.DeliveryHour = tc.DeliveryHour

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
