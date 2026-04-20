package models_test

import (
	"testing"
	"time"

	"github.com/nyaruka/gocommon/i18n"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/assert"
)

func TestGetExpired(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetData)

	optIn := testdata.InsertOptIn(rt, testdata.Org1, "Polls")

	// add a schedule and tie a broadcast to it
	s1 := testdata.InsertSchedule(rt, testdata.Org1, models.RepeatPeriodNever, time.Now().Add(-24*time.Hour))

	testdata.InsertBroadcast(rt, testdata.Org1, "eng", map[i18n.Language]string{"eng": "Test message", "fra": "Un Message"}, optIn, s1,
		[]*testdata.Contact{testdata.Cathy, testdata.George}, []*testdata.Group{testdata.DoctorsGroup},
	)

	// add another and tie a trigger to it
	s2 := testdata.InsertSchedule(rt, testdata.Org1, models.RepeatPeriodNever, time.Now().Add(-48*time.Hour))

	testdata.InsertScheduledTrigger(rt, testdata.Org1, testdata.Favorites, s2, []*testdata.Group{testdata.DoctorsGroup}, nil, []*testdata.Contact{testdata.Cathy, testdata.George})

	s3 := testdata.InsertSchedule(rt, testdata.Org1, models.RepeatPeriodNever, time.Now().Add(-72*time.Hour))

	// get expired schedules
	schedules, err := models.GetUnfiredSchedules(ctx, rt.DB.DB)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(schedules))

	assert.Equal(t, s3, schedules[0].ID())
	assert.Nil(t, schedules[0].Broadcast())
	assert.Equal(t, models.RepeatPeriodNever, schedules[0].RepeatPeriod())
	assert.NotNil(t, schedules[0].NextFire())
	assert.Nil(t, schedules[0].LastFire())

	assert.Equal(t, s2, schedules[1].ID())
	assert.Nil(t, schedules[1].Broadcast())

	trigger := schedules[1].Trigger()
	assert.NotNil(t, trigger)
	assert.Equal(t, testdata.Favorites.ID, trigger.FlowID())
	assert.Equal(t, testdata.Org1.ID, trigger.OrgID())
	assert.Equal(t, []models.ContactID{testdata.Cathy.ID, testdata.George.ID}, trigger.ContactIDs())
	assert.Equal(t, []models.GroupID{testdata.DoctorsGroup.ID}, trigger.IncludeGroupIDs())

	assert.Equal(t, s1, schedules[2].ID())
	bcast := schedules[2].Broadcast()
	assert.NotNil(t, bcast)
	assert.Equal(t, i18n.Language("eng"), bcast.BaseLanguage)
	assert.Equal(t, models.TemplateStateUnevaluated, bcast.TemplateState)
	assert.Equal(t, "Test message", bcast.Translations["eng"].Text)
	assert.Equal(t, "Un Message", bcast.Translations["fra"].Text)
	assert.Equal(t, optIn.ID, bcast.OptInID)
	assert.Equal(t, testdata.Org1.ID, bcast.OrgID)
	assert.Equal(t, []models.ContactID{testdata.Cathy.ID, testdata.George.ID}, bcast.ContactIDs)
	assert.Equal(t, []models.GroupID{testdata.DoctorsGroup.ID}, bcast.GroupIDs)
}

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
		Now          time.Time
		Location     *time.Location
		Period       models.RepeatPeriod
		HourOfDay    *int
		MinuteOfHour *int
		DayOfMonth   *int
		DaysOfWeek   string
		Next         []*time.Time
		Error        string
	}{
		{
			Label:    "no hour of day set",
			Now:      time.Date(2019, 8, 20, 10, 57, 0, 0, la),
			Location: la,
			Period:   models.RepeatPeriodDaily,
			Error:    "schedule 0 has no repeat_hour_of_day set",
		},
		{
			Label:     "no minute of hour set",
			Now:       time.Date(2019, 8, 20, 10, 57, 0, 0, la),
			Location:  la,
			Period:    models.RepeatPeriodDaily,
			HourOfDay: ip(12),
			Error:     "schedule 0 has no repeat_minute_of_hour set",
		},
		{
			Label:        "unknown repeat period",
			Now:          time.Date(2019, 8, 20, 10, 57, 0, 0, la),
			Location:     la,
			Period:       "Z",
			HourOfDay:    ip(12),
			MinuteOfHour: ip(35),
			Error:        "unknown repeat period: Z",
		},
		{
			Label:        "no repeat",
			Now:          time.Date(2019, 8, 20, 10, 57, 0, 0, la),
			Location:     la,
			Period:       models.RepeatPeriodNever,
			HourOfDay:    ip(12),
			MinuteOfHour: ip(35),
			Next:         nil,
		},
		{
			Label:        "daily repeat on same day",
			Now:          time.Date(2019, 8, 20, 10, 57, 0, 0, la),
			Location:     la,
			Period:       models.RepeatPeriodDaily,
			HourOfDay:    ip(12),
			MinuteOfHour: ip(35),
			Next:         []*time.Time{dp(2019, 8, 20, 12, 35, la)},
		},
		{
			Label:        "daily repeat on same hour minute",
			Now:          time.Date(2019, 8, 20, 12, 35, 0, 0, la),
			Location:     la,
			Period:       models.RepeatPeriodDaily,
			HourOfDay:    ip(12),
			MinuteOfHour: ip(35),
			Next:         []*time.Time{dp(2019, 8, 21, 12, 35, la)},
		},
		{
			Label:        "daily repeat for next day",
			Now:          time.Date(2019, 8, 20, 13, 57, 0, 0, la),
			Location:     la,
			Period:       models.RepeatPeriodDaily,
			HourOfDay:    ip(12),
			MinuteOfHour: ip(35),
			Next:         []*time.Time{dp(2019, 8, 21, 12, 35, la)},
		},
		{
			Label:        "daily repeat for next day across DST start",
			Now:          time.Date(2019, 3, 9, 12, 30, 0, 0, la),
			Location:     la,
			Period:       models.RepeatPeriodDaily,
			HourOfDay:    ip(12),
			MinuteOfHour: ip(30),
			Next: []*time.Time{
				dp(2019, 3, 10, 12, 30, la),
				dp(2019, 3, 11, 12, 30, la),
			},
		},
		{
			Label:        "daily repeat for next day across DST end",
			Now:          time.Date(2019, 11, 2, 12, 30, 0, 0, la),
			Location:     la,
			Period:       models.RepeatPeriodDaily,
			HourOfDay:    ip(12),
			MinuteOfHour: ip(30),
			Next: []*time.Time{
				dp(2019, 11, 3, 12, 30, la),
				dp(2019, 11, 4, 12, 30, la),
			},
		},
		{
			Label:        "weekly repeat missing days of week",
			Now:          time.Date(2019, 8, 20, 13, 57, 0, 0, la),
			Location:     la,
			Period:       models.RepeatPeriodWeekly,
			HourOfDay:    ip(12),
			MinuteOfHour: ip(35),
			Error:        "schedule 0 repeats weekly but has no repeat_days_of_week",
		},
		{
			Label:        "weekly with invalid days of week",
			Now:          time.Date(2019, 8, 20, 13, 57, 0, 0, la),
			Location:     la,
			Period:       models.RepeatPeriodWeekly,
			HourOfDay:    ip(12),
			MinuteOfHour: ip(35),
			DaysOfWeek:   "Z",
			Error:        "schedule 0 has unknown day of week: Z",
		},
		{
			Label:        "weekly repeat to day later in week",
			Now:          time.Date(2019, 8, 20, 13, 57, 0, 0, la),
			Location:     la,
			Period:       models.RepeatPeriodWeekly,
			HourOfDay:    ip(12),
			MinuteOfHour: ip(35),
			DaysOfWeek:   "RU",
			Next: []*time.Time{
				dp(2019, 8, 22, 12, 35, la),
				dp(2019, 8, 25, 12, 35, la),
				dp(2019, 8, 29, 12, 35, la),
			},
		},
		{
			Label:        "weekly repeat to day later in week using fire date",
			Now:          time.Date(2019, 8, 26, 12, 35, 0, 0, la),
			Location:     la,
			Period:       models.RepeatPeriodWeekly,
			HourOfDay:    ip(12),
			MinuteOfHour: ip(35),
			DaysOfWeek:   "MTWRFSU",
			Next: []*time.Time{
				dp(2019, 8, 27, 12, 35, la),
				dp(2019, 8, 28, 12, 35, la),
				dp(2019, 8, 29, 12, 35, la),
				dp(2019, 8, 30, 12, 35, la),
				dp(2019, 8, 31, 12, 35, la),
				dp(2019, 9, 1, 12, 35, la),
			},
		},
		{
			Label:        "weekly repeat for next day across DST",
			Now:          time.Date(2019, 3, 9, 12, 30, 0, 0, la),
			Location:     la,
			Period:       models.RepeatPeriodWeekly,
			HourOfDay:    ip(12),
			MinuteOfHour: ip(30),
			DaysOfWeek:   "MTWRFSU",
			Next:         []*time.Time{dp(2019, 3, 10, 12, 30, la)},
		},
		{
			Label:        "weekly repeat to day in next week",
			Now:          time.Date(2019, 8, 20, 13, 57, 0, 0, la),
			Location:     la,
			Period:       models.RepeatPeriodWeekly,
			HourOfDay:    ip(12),
			MinuteOfHour: ip(35),
			DaysOfWeek:   "M",
			Next:         []*time.Time{dp(2019, 8, 26, 12, 35, la)},
		},
		{
			Label:        "monthly repeat with no day of month set",
			Now:          time.Date(2019, 8, 20, 13, 57, 0, 0, la),
			Location:     la,
			Period:       models.RepeatPeriodMonthly,
			HourOfDay:    ip(12),
			MinuteOfHour: ip(35),
			Error:        "schedule 0 repeats monthly but has no repeat_day_of_month",
		},
		{
			Label:        "monthly repeat to day in same month",
			Now:          time.Date(2019, 8, 20, 13, 57, 0, 0, la),
			Location:     la,
			Period:       models.RepeatPeriodMonthly,
			HourOfDay:    ip(12),
			MinuteOfHour: ip(35),
			DayOfMonth:   ip(31),
			Next: []*time.Time{
				dp(2019, 8, 31, 12, 35, la),
				dp(2019, 9, 30, 12, 35, la),
				dp(2019, 10, 31, 12, 35, la),
				dp(2019, 11, 30, 12, 35, la),
			},
		},
		{
			Label:        "monthly repeat to day in same month from fire date",
			Now:          time.Date(2019, 8, 20, 12, 35, 0, 0, la),
			Location:     la,
			Period:       models.RepeatPeriodMonthly,
			HourOfDay:    ip(12),
			MinuteOfHour: ip(35),
			DayOfMonth:   ip(20),
			Next:         []*time.Time{dp(2019, 9, 20, 12, 35, la)},
		},
		{
			Label:        "monthly repeat to day in next month",
			Now:          time.Date(2019, 8, 20, 13, 57, 0, 0, la),
			Location:     la,
			Period:       models.RepeatPeriodMonthly,
			HourOfDay:    ip(12),
			MinuteOfHour: ip(35),
			DayOfMonth:   ip(5),
			Next:         []*time.Time{dp(2019, 9, 5, 12, 35, la)},
		},
		{
			Label:        "monthly repeat to day that exceeds month",
			Now:          time.Date(2019, 9, 20, 13, 57, 0, 0, la),
			Location:     la,
			Period:       models.RepeatPeriodMonthly,
			HourOfDay:    ip(12),
			MinuteOfHour: ip(35),
			DayOfMonth:   ip(31),
			Next:         []*time.Time{dp(2019, 9, 30, 12, 35, la)},
		},
		{
			Label:        "monthly repeat to day in next month that exceeds month",
			Now:          time.Date(2019, 8, 31, 13, 57, 0, 0, la),
			Location:     la,
			Period:       models.RepeatPeriodMonthly,
			HourOfDay:    ip(12),
			MinuteOfHour: ip(35),
			DayOfMonth:   ip(31),
			Next:         []*time.Time{dp(2019, 9, 30, 12, 35, la)},
		},
		{
			Label:        "monthy repeat for next month across DST",
			Now:          time.Date(2019, 2, 10, 12, 30, 0, 0, la),
			Location:     la,
			Period:       models.RepeatPeriodMonthly,
			HourOfDay:    ip(12),
			MinuteOfHour: ip(30),
			DayOfMonth:   ip(10),
			Next:         []*time.Time{dp(2019, 3, 10, 12, 30, la)},
		},
	}

tests:
	for _, tc := range tcs {
		// create a fake schedule
		sched := models.NewSchedule(tc.Period, tc.HourOfDay, tc.MinuteOfHour, tc.DayOfMonth, tc.DaysOfWeek)
		now := tc.Now

		for _, n := range tc.Next {
			next, err := sched.GetNextFire(tc.Location, now)
			if err != nil {
				if tc.Error == "" {
					assert.NoError(t, err, "%s: received unexpected error", tc.Label)
					continue tests
				}
				assert.Equal(t, tc.Error, err.Error(), "%s: error did not match", tc.Label)
				continue tests
			}
			assert.Equal(t, n, next, "%s: next fire did not match", tc.Label)

			if n != nil {
				now = *n
			}
		}
	}
}
