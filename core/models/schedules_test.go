package models

import (
	"testing"
	"time"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/null"
	"github.com/stretchr/testify/assert"
)

func TestGetExpired(t *testing.T) {
	ctx := testsuite.CTX()

	// add a schedule and tie a broadcast to it
	db := testsuite.DB()
	var s1 ScheduleID
	err := db.Get(
		&s1,
		`INSERT INTO schedules_schedule(is_active, repeat_period, created_on, modified_on, next_fire, created_by_id, modified_by_id, org_id)
			VALUES(TRUE, 'O', NOW(), NOW(), NOW()- INTERVAL '1 DAY', 1, 1, $1) RETURNING id`,
		Org1,
	)
	assert.NoError(t, err)
	var b1 BroadcastID
	err = db.Get(
		&b1,
		`INSERT INTO msgs_broadcast(status, text, base_language, is_active, created_on, modified_on, send_all, created_by_id, modified_by_id, org_id, schedule_id)
			VALUES('P', hstore(ARRAY['eng','Test message', 'fra', 'Un Message']), 'eng', TRUE, NOW(), NOW(), TRUE, 1, 1, $1, $2) RETURNING id`,
		Org1, s1,
	)
	assert.NoError(t, err)

	// add a few contacts to the broadcast
	db.MustExec(`INSERT INTO msgs_broadcast_contacts(broadcast_id, contact_id) VALUES($1, $2),($1, $3)`, b1, CathyID, GeorgeID)

	// and a group
	db.MustExec(`INSERT INTO msgs_broadcast_groups(broadcast_id, contactgroup_id) VALUES($1, $2)`, b1, DoctorsGroupID)

	// and a URN
	db.MustExec(`INSERT INTO msgs_broadcast_urns(broadcast_id, contacturn_id) VALUES($1, $2)`, b1, CathyURNID)

	// add another and tie a trigger to it
	var s2 ScheduleID
	err = db.Get(
		&s2,
		`INSERT INTO schedules_schedule(is_active, repeat_period, created_on, modified_on, next_fire, created_by_id, modified_by_id, org_id)
			VALUES(TRUE, 'O', NOW(), NOW(), NOW()- INTERVAL '2 DAY', 1, 1, $1) RETURNING id`,
		Org1,
	)
	assert.NoError(t, err)
	var t1 TriggerID
	err = db.Get(
		&t1,
		`INSERT INTO triggers_trigger(is_active, created_on, modified_on, is_archived, trigger_type, created_by_id, modified_by_id, org_id, flow_id, schedule_id)
			VALUES(TRUE, NOW(), NOW(), FALSE, 'S', 1, 1, $1, $2, $3) RETURNING id`,
		Org1, FavoritesFlowID, s2,
	)
	assert.NoError(t, err)

	// add a few contacts to the trigger
	db.MustExec(`INSERT INTO triggers_trigger_contacts(trigger_id, contact_id) VALUES($1, $2),($1, $3)`, t1, CathyID, GeorgeID)

	// and a group
	db.MustExec(`INSERT INTO triggers_trigger_groups(trigger_id, contactgroup_id) VALUES($1, $2)`, t1, DoctorsGroupID)

	var s3 ScheduleID
	err = db.Get(
		&s3,
		`INSERT INTO schedules_schedule(is_active, repeat_period, created_on, modified_on, next_fire, created_by_id, modified_by_id, org_id)
			VALUES(TRUE, 'O', NOW(), NOW(), NOW()- INTERVAL '3 DAY', 1, 1, $1) RETURNING id`,
		Org1,
	)
	assert.NoError(t, err)

	// get expired schedules
	schedules, err := GetUnfiredSchedules(ctx, db)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(schedules))

	assert.Equal(t, s3, schedules[0].ID())
	assert.Nil(t, schedules[0].Broadcast())
	assert.Equal(t, RepeatPeriodNever, schedules[0].s.RepeatPeriod)
	assert.NotNil(t, schedules[0].s.NextFire)
	assert.Nil(t, schedules[0].s.LastFire)

	assert.Equal(t, s2, schedules[1].ID())
	assert.Nil(t, schedules[1].Broadcast())
	start := schedules[1].FlowStart()
	assert.NotNil(t, start)
	assert.Equal(t, MessagingFlow, start.FlowType())
	assert.Equal(t, FavoritesFlowID, start.FlowID())
	assert.Equal(t, Org1, start.OrgID())
	assert.Equal(t, []ContactID{CathyID, GeorgeID}, start.ContactIDs())
	assert.Equal(t, []GroupID{DoctorsGroupID}, start.GroupIDs())

	assert.Equal(t, s1, schedules[2].ID())
	bcast := schedules[2].Broadcast()
	assert.NotNil(t, bcast)
	assert.Equal(t, envs.Language("eng"), bcast.BaseLanguage())
	assert.Equal(t, TemplateStateUnevaluated, bcast.TemplateState())
	assert.Equal(t, "Test message", bcast.Translations()["eng"].Text)
	assert.Equal(t, "Un Message", bcast.Translations()["fra"].Text)
	assert.Equal(t, Org1, bcast.OrgID())
	assert.Equal(t, []ContactID{CathyID, GeorgeID}, bcast.ContactIDs())
	assert.Equal(t, []GroupID{DoctorsGroupID}, bcast.GroupIDs())
	assert.Equal(t, []urns.URN{urns.URN("tel:+16055741111?id=10000")}, bcast.URNs())
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
		Period       RepeatPeriod
		HourOfDay    *int
		MinuteOfHour *int
		DayOfMonth   *int
		DaysOfWeek   null.String
		Next         []*time.Time
		Error        string
	}{
		{
			Label:    "no hour of day set",
			Now:      time.Date(2019, 8, 20, 10, 57, 0, 0, la),
			Location: la,
			Period:   RepeatPeriodDaily,
			Error:    "schedule 0 has no repeat_hour_of_day set",
		},
		{
			Label:     "no minute of hour set",
			Now:       time.Date(2019, 8, 20, 10, 57, 0, 0, la),
			Location:  la,
			Period:    RepeatPeriodDaily,
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
			Period:       RepeatPeriodNever,
			HourOfDay:    ip(12),
			MinuteOfHour: ip(35),
			Next:         nil,
		},
		{
			Label:        "daily repeat on same day",
			Now:          time.Date(2019, 8, 20, 10, 57, 0, 0, la),
			Location:     la,
			Period:       RepeatPeriodDaily,
			HourOfDay:    ip(12),
			MinuteOfHour: ip(35),
			Next:         []*time.Time{dp(2019, 8, 20, 12, 35, la)},
		},
		{
			Label:        "daily repeat on same hour minute",
			Now:          time.Date(2019, 8, 20, 12, 35, 0, 0, la),
			Location:     la,
			Period:       RepeatPeriodDaily,
			HourOfDay:    ip(12),
			MinuteOfHour: ip(35),
			Next:         []*time.Time{dp(2019, 8, 21, 12, 35, la)},
		},
		{
			Label:        "daily repeat for next day",
			Now:          time.Date(2019, 8, 20, 13, 57, 0, 0, la),
			Location:     la,
			Period:       RepeatPeriodDaily,
			HourOfDay:    ip(12),
			MinuteOfHour: ip(35),
			Next:         []*time.Time{dp(2019, 8, 21, 12, 35, la)},
		},
		{
			Label:        "daily repeat for next day across DST start",
			Now:          time.Date(2019, 3, 9, 12, 30, 0, 0, la),
			Location:     la,
			Period:       RepeatPeriodDaily,
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
			Period:       RepeatPeriodDaily,
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
			Period:       RepeatPeriodWeekly,
			HourOfDay:    ip(12),
			MinuteOfHour: ip(35),
			Error:        "schedule 0 repeats weekly but has no repeat_days_of_week",
		},
		{
			Label:        "weekly with invalid days of week",
			Now:          time.Date(2019, 8, 20, 13, 57, 0, 0, la),
			Location:     la,
			Period:       RepeatPeriodWeekly,
			HourOfDay:    ip(12),
			MinuteOfHour: ip(35),
			DaysOfWeek:   null.String("Z"),
			Error:        "schedule 0 has unknown day of week: Z",
		},
		{
			Label:        "weekly repeat to day later in week",
			Now:          time.Date(2019, 8, 20, 13, 57, 0, 0, la),
			Location:     la,
			Period:       RepeatPeriodWeekly,
			HourOfDay:    ip(12),
			MinuteOfHour: ip(35),
			DaysOfWeek:   null.String("RU"),
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
			Period:       RepeatPeriodWeekly,
			HourOfDay:    ip(12),
			MinuteOfHour: ip(35),
			DaysOfWeek:   null.String("MTWRFSU"),
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
			Period:       RepeatPeriodWeekly,
			HourOfDay:    ip(12),
			MinuteOfHour: ip(30),
			DaysOfWeek:   null.String("MTWRFSU"),
			Next:         []*time.Time{dp(2019, 3, 10, 12, 30, la)},
		},
		{
			Label:        "weekly repeat to day in next week",
			Now:          time.Date(2019, 8, 20, 13, 57, 0, 0, la),
			Location:     la,
			Period:       RepeatPeriodWeekly,
			HourOfDay:    ip(12),
			MinuteOfHour: ip(35),
			DaysOfWeek:   null.String("M"),
			Next:         []*time.Time{dp(2019, 8, 26, 12, 35, la)},
		},
		{
			Label:        "monthly repeat with no day of month set",
			Now:          time.Date(2019, 8, 20, 13, 57, 0, 0, la),
			Location:     la,
			Period:       RepeatPeriodMonthly,
			HourOfDay:    ip(12),
			MinuteOfHour: ip(35),
			Error:        "schedule 0 repeats monthly but has no repeat_day_of_month",
		},
		{
			Label:        "monthly repeat to day in same month",
			Now:          time.Date(2019, 8, 20, 13, 57, 0, 0, la),
			Location:     la,
			Period:       RepeatPeriodMonthly,
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
			Period:       RepeatPeriodMonthly,
			HourOfDay:    ip(12),
			MinuteOfHour: ip(35),
			DayOfMonth:   ip(20),
			Next:         []*time.Time{dp(2019, 9, 20, 12, 35, la)},
		},
		{
			Label:        "monthly repeat to day in next month",
			Now:          time.Date(2019, 8, 20, 13, 57, 0, 0, la),
			Location:     la,
			Period:       RepeatPeriodMonthly,
			HourOfDay:    ip(12),
			MinuteOfHour: ip(35),
			DayOfMonth:   ip(5),
			Next:         []*time.Time{dp(2019, 9, 5, 12, 35, la)},
		},
		{
			Label:        "monthly repeat to day that exceeds month",
			Now:          time.Date(2019, 9, 20, 13, 57, 0, 0, la),
			Location:     la,
			Period:       RepeatPeriodMonthly,
			HourOfDay:    ip(12),
			MinuteOfHour: ip(35),
			DayOfMonth:   ip(31),
			Next:         []*time.Time{dp(2019, 9, 30, 12, 35, la)},
		},
		{
			Label:        "monthly repeat to day in next month that exceeds month",
			Now:          time.Date(2019, 8, 31, 13, 57, 0, 0, la),
			Location:     la,
			Period:       RepeatPeriodMonthly,
			HourOfDay:    ip(12),
			MinuteOfHour: ip(35),
			DayOfMonth:   ip(31),
			Next:         []*time.Time{dp(2019, 9, 30, 12, 35, la)},
		},
		{
			Label:        "monthy repeat for next month across DST",
			Now:          time.Date(2019, 2, 10, 12, 30, 0, 0, la),
			Location:     la,
			Period:       RepeatPeriodMonthly,
			HourOfDay:    ip(12),
			MinuteOfHour: ip(30),
			DayOfMonth:   ip(10),
			Next:         []*time.Time{dp(2019, 3, 10, 12, 30, la)},
		},
	}

tests:
	for _, tc := range tcs {
		// create a fake schedule
		sched := &Schedule{}
		s := &sched.s
		s.RepeatPeriod = tc.Period
		s.HourOfDay = tc.HourOfDay
		s.MinuteOfHour = tc.MinuteOfHour
		s.DayOfMonth = tc.DayOfMonth
		s.DaysOfWeek = tc.DaysOfWeek

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
