package models

import (
	"testing"
	"time"

	"github.com/nyaruka/null"
	"github.com/stretchr/testify/assert"
)

func TestNextFire(t *testing.T) {
	la, err := time.LoadLocation("America/Los_Angeles")
	assert.NoError(t, err)

	dp := func(year int, month int, day int, hour int, minute int, tz *time.Location) *time.Time {
		d := time.Date(year, time.Month(month), day, hour, minute, 0, 0, tz)
		return &d
	}

	ip := func(i int) *int {
		return &i
	}

	tcs := []struct {
		Label        string
		Start        time.Time
		Location     *time.Location
		Period       RepeatPeriod
		HourOfDay    *int
		MinuteOfHour *int
		DayOfMonth   *int
		DaysOfWeek   null.String
		Next         *time.Time
		Error        string
	}{
		{
			Label:    "no hour of day set",
			Start:    time.Date(2019, 8, 20, 10, 57, 0, 0, la),
			Location: la,
			Period:   RepeatPeriodDaily,
			Error:    "schedule 0 has no repeat_hour_of_day set",
		},
		{
			Label:     "no minute of hour set",
			Start:     time.Date(2019, 8, 20, 10, 57, 0, 0, la),
			Location:  la,
			Period:    RepeatPeriodDaily,
			HourOfDay: ip(12),
			Error:     "schedule 0 has no repeat_minute_of_hour set",
		},
		{
			Label:        "unknown repeat period",
			Start:        time.Date(2019, 8, 20, 10, 57, 0, 0, la),
			Location:     la,
			Period:       "Z",
			HourOfDay:    ip(12),
			MinuteOfHour: ip(35),
			Error:        "unknown repeat period: Z",
		},
		{
			Label:        "no repeat",
			Start:        time.Date(2019, 8, 20, 10, 57, 0, 0, la),
			Location:     la,
			Period:       RepeatPeriodNever,
			HourOfDay:    ip(12),
			MinuteOfHour: ip(35),
			Next:         dp(2019, 8, 20, 12, 35, la),
		},
		{
			Label:        "daily repeat on same day",
			Start:        time.Date(2019, 8, 20, 10, 57, 0, 0, la),
			Location:     la,
			Period:       RepeatPeriodDaily,
			HourOfDay:    ip(12),
			MinuteOfHour: ip(35),
			Next:         dp(2019, 8, 20, 12, 35, la),
		},
		{
			Label:        "daily repeat on same hour minute",
			Start:        time.Date(2019, 8, 20, 12, 35, 0, 0, la),
			Location:     la,
			Period:       RepeatPeriodDaily,
			HourOfDay:    ip(12),
			MinuteOfHour: ip(35),
			Next:         dp(2019, 8, 21, 12, 35, la),
		},
		{
			Label:        "daily repeat for next day",
			Start:        time.Date(2019, 8, 20, 13, 57, 0, 0, la),
			Location:     la,
			Period:       RepeatPeriodDaily,
			HourOfDay:    ip(12),
			MinuteOfHour: ip(35),
			Next:         dp(2019, 8, 21, 12, 35, la),
		},
		{
			Label:        "daily repeat for next day across DST",
			Start:        time.Date(2019, 3, 9, 12, 30, 0, 0, la),
			Location:     la,
			Period:       RepeatPeriodDaily,
			HourOfDay:    ip(12),
			MinuteOfHour: ip(30),
			Next:         dp(2019, 3, 10, 12, 30, la),
		},
		{
			Label:        "weekly repeat missing days of week",
			Start:        time.Date(2019, 8, 20, 13, 57, 0, 0, la),
			Location:     la,
			Period:       RepeatPeriodWeekly,
			HourOfDay:    ip(12),
			MinuteOfHour: ip(35),
			Error:        "schedule 0 repeats weekly but has no repeat_days_of_week",
		},
		{
			Label:        "weekly with invalid days of week",
			Start:        time.Date(2019, 8, 20, 13, 57, 0, 0, la),
			Location:     la,
			Period:       RepeatPeriodWeekly,
			HourOfDay:    ip(12),
			MinuteOfHour: ip(35),
			DaysOfWeek:   null.String("Z"),
			Error:        "schedule 0 has unknown day of week: Z",
		},
		{
			Label:        "weekly repeat to day later in week",
			Start:        time.Date(2019, 8, 20, 13, 57, 0, 0, la),
			Location:     la,
			Period:       RepeatPeriodWeekly,
			HourOfDay:    ip(12),
			MinuteOfHour: ip(35),
			DaysOfWeek:   null.String("RU"),
			Next:         dp(2019, 8, 22, 12, 35, la),
		},
		{
			Label:        "weekly repeat to day later in week using fire date",
			Start:        time.Date(2019, 8, 20, 12, 35, 0, 0, la),
			Location:     la,
			Period:       RepeatPeriodWeekly,
			HourOfDay:    ip(12),
			MinuteOfHour: ip(35),
			DaysOfWeek:   null.String("MTWRFSU"),
			Next:         dp(2019, 8, 21, 12, 35, la),
		},
		{
			Label:        "weekly repeat for next day across DST",
			Start:        time.Date(2019, 3, 9, 12, 30, 0, 0, la),
			Location:     la,
			Period:       RepeatPeriodWeekly,
			HourOfDay:    ip(12),
			MinuteOfHour: ip(30),
			DaysOfWeek:   null.String("MTWRFSU"),
			Next:         dp(2019, 3, 10, 12, 30, la),
		},
		{
			Label:        "weekly repeat to day in next week",
			Start:        time.Date(2019, 8, 20, 13, 57, 0, 0, la),
			Location:     la,
			Period:       RepeatPeriodWeekly,
			HourOfDay:    ip(12),
			MinuteOfHour: ip(35),
			DaysOfWeek:   null.String("M"),
			Next:         dp(2019, 8, 26, 12, 35, la),
		},
		{
			Label:        "monthly repeat with no day of month set",
			Start:        time.Date(2019, 8, 20, 13, 57, 0, 0, la),
			Location:     la,
			Period:       RepeatPeriodMonthly,
			HourOfDay:    ip(12),
			MinuteOfHour: ip(35),
			Error:        "schedule 0 repeats monthly but has no repeat_day_of_month",
		},
		{
			Label:        "monthly repeat to day in same month",
			Start:        time.Date(2019, 8, 20, 13, 57, 0, 0, la),
			Location:     la,
			Period:       RepeatPeriodMonthly,
			HourOfDay:    ip(12),
			MinuteOfHour: ip(35),
			DayOfMonth:   ip(31),
			Next:         dp(2019, 8, 31, 12, 35, la),
		},
		{
			Label:        "monthly repeat to day in same month from fire date",
			Start:        time.Date(2019, 8, 20, 12, 35, 0, 0, la),
			Location:     la,
			Period:       RepeatPeriodMonthly,
			HourOfDay:    ip(12),
			MinuteOfHour: ip(35),
			DayOfMonth:   ip(20),
			Next:         dp(2019, 9, 20, 12, 35, la),
		},
		{
			Label:        "monthly repeat to day in next month",
			Start:        time.Date(2019, 8, 20, 13, 57, 0, 0, la),
			Location:     la,
			Period:       RepeatPeriodMonthly,
			HourOfDay:    ip(12),
			MinuteOfHour: ip(35),
			DayOfMonth:   ip(5),
			Next:         dp(2019, 9, 5, 12, 35, la),
		},
		{
			Label:        "monthly repeat to day that exceeds month",
			Start:        time.Date(2019, 9, 20, 13, 57, 0, 0, la),
			Location:     la,
			Period:       RepeatPeriodMonthly,
			HourOfDay:    ip(12),
			MinuteOfHour: ip(35),
			DayOfMonth:   ip(31),
			Next:         dp(2019, 9, 30, 12, 35, la),
		},
		{
			Label:        "monthly repeat to day in next month that exceeds month",
			Start:        time.Date(2019, 8, 31, 13, 57, 0, 0, la),
			Location:     la,
			Period:       RepeatPeriodMonthly,
			HourOfDay:    ip(12),
			MinuteOfHour: ip(35),
			DayOfMonth:   ip(31),
			Next:         dp(2019, 9, 30, 12, 35, la),
		},
		{
			Label:        "monthy repeat for next month across DST",
			Start:        time.Date(2019, 2, 10, 12, 30, 0, 0, la),
			Location:     la,
			Period:       RepeatPeriodMonthly,
			HourOfDay:    ip(12),
			MinuteOfHour: ip(30),
			DayOfMonth:   ip(10),
			Next:         dp(2019, 3, 10, 12, 30, la),
		},
	}

	for _, tc := range tcs {
		// create a fake schedule
		sched := &Schedule{}
		s := &sched.s
		s.RepeatPeriod = tc.Period
		s.HourOfDay = tc.HourOfDay
		s.MinuteOfHour = tc.MinuteOfHour
		s.DayOfMonth = tc.DayOfMonth
		s.DaysOfWeek = tc.DaysOfWeek

		start := tc.Start.In(time.UTC)

		next, err := sched.GetNextFire(tc.Location, start)
		if err != nil {
			if tc.Error == "" {
				assert.NoError(t, err, "%s: received unexpected error", tc.Label)
				continue
			}
			assert.Equal(t, tc.Error, err.Error(), "%s: error did not match", tc.Label)
			continue
		}
		assert.Equal(t, tc.Next, next, "%s: next fire did not match", tc.Label)
	}
}
