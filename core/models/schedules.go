package models

import (
	"context"
	"database/sql/driver"
	"fmt"
	"time"

	"github.com/nyaruka/gocommon/dbutil"
	"github.com/nyaruka/null"

	"github.com/pkg/errors"
)

// ScheduleID is our internal type for schedule IDs
type ScheduleID null.Int

// NilScheduleID is our constant for a nil schedule id
const NilScheduleID = ScheduleID(0)

type RepeatPeriod string

const RepeatPeriodNever = RepeatPeriod("O")
const RepeatPeriodDaily = RepeatPeriod("D")
const RepeatPeriodWeekly = RepeatPeriod("W")
const RepeatPeriodMonthly = RepeatPeriod("M")

const Monday = 'M'
const Tuesday = 'T'
const Wednesday = 'W'
const Thursday = 'R'
const Friday = 'F'
const Saturday = 'S'
const Sunday = 'U'

var dayStrToDayInt = map[byte]time.Weekday{
	Sunday:    0,
	Monday:    1,
	Tuesday:   2,
	Wednesday: 3,
	Thursday:  4,
	Friday:    5,
	Saturday:  6,
}

// Schedule represents a scheduled event
type Schedule struct {
	s struct {
		ID           ScheduleID   `json:"id"`
		RepeatPeriod RepeatPeriod `json:"repeat_period"`
		HourOfDay    *int         `json:"repeat_hour_of_day"`
		MinuteOfHour *int         `json:"repeat_minute_of_hour"`
		DayOfMonth   *int         `json:"repeat_day_of_month"`
		DaysOfWeek   null.String  `json:"repeat_days_of_week"`
		NextFire     *time.Time   `json:"next_fire"`
		LastFire     *time.Time   `json:"last_fire"`
		OrgID        OrgID        `json:"org_id"`

		// Timezone of our org
		Timezone string `json:"timezone"`

		// associated broadcast if any
		Broadcast *Broadcast `json:"broadcast,omitempty"`

		// associated trigger in flowstart format
		FlowStart *FlowStart `json:"start,omitempty"`
	}
}

func NewSchedule(period RepeatPeriod, hourOfDay, minuteOfHour, dayOfMonth *int, daysOfWeek string) *Schedule {
	sched := &Schedule{}
	s := &sched.s
	s.RepeatPeriod = period
	s.HourOfDay = hourOfDay
	s.MinuteOfHour = minuteOfHour
	s.DayOfMonth = dayOfMonth
	s.DaysOfWeek = null.String(daysOfWeek)
	return sched
}

func (s *Schedule) ID() ScheduleID             { return s.s.ID }
func (s *Schedule) OrgID() OrgID               { return s.s.OrgID }
func (s *Schedule) Broadcast() *Broadcast      { return s.s.Broadcast }
func (s *Schedule) FlowStart() *FlowStart      { return s.s.FlowStart }
func (s *Schedule) RepeatPeriod() RepeatPeriod { return s.s.RepeatPeriod }
func (s *Schedule) NextFire() *time.Time       { return s.s.NextFire }
func (s *Schedule) LastFire() *time.Time       { return s.s.LastFire }
func (s *Schedule) Timezone() (*time.Location, error) {
	return time.LoadLocation(s.s.Timezone)
}

// UpdateFires updates the next and last fire for a shedule on the db
func (s *Schedule) UpdateFires(ctx context.Context, tx Queryer, last time.Time, next *time.Time) error {
	_, err := tx.ExecContext(ctx, `UPDATE schedules_schedule SET last_fire = $2, next_fire = $3 WHERE id = $1`,
		s.s.ID, last, next,
	)
	if err != nil {
		return errors.Wrapf(err, "error updating schedule fire dates for: %d", s.s.ID)
	}
	return nil
}

// GetNextFire returns the next fire for this schedule (if any)
func (s *Schedule) GetNextFire(tz *time.Location, now time.Time) (*time.Time, error) {
	// Never repeats? no next fire
	if s.s.RepeatPeriod == RepeatPeriodNever {
		return nil, nil
	}

	// should have hour and minute on everything else
	if s.s.HourOfDay == nil {
		return nil, errors.Errorf("schedule %d has no repeat_hour_of_day set", s.s.ID)
	}
	if s.s.MinuteOfHour == nil {
		return nil, errors.Errorf("schedule %d has no repeat_minute_of_hour set", s.s.ID)
	}

	// increment now by a minute, we don't want to double schedule in case of small clock drifts between boxes or db
	now = now.Add(time.Minute)

	// change our time to be in our location
	start := now.In(tz)
	minute := *s.s.MinuteOfHour
	hour := *s.s.HourOfDay

	// set our next fire to today at the specified hour and minute
	next := time.Date(start.Year(), start.Month(), start.Day(), hour, minute, 0, 0, tz)

	switch s.s.RepeatPeriod {

	case RepeatPeriodDaily:
		for !next.After(now) {
			next = next.AddDate(0, 0, 1)
		}
		return &next, nil

	case RepeatPeriodWeekly:
		if s.s.DaysOfWeek == "" {
			return nil, errors.Errorf("schedule %d repeats weekly but has no repeat_days_of_week", s.s.ID)
		}

		// build a map of the days we send on
		sendDays := make(map[time.Weekday]bool)
		for i := 0; i < len(s.s.DaysOfWeek); i++ {
			day, found := dayStrToDayInt[s.s.DaysOfWeek[i]]
			if !found {
				return nil, errors.Errorf("schedule %d has unknown day of week: %s", s.s.ID, string(s.s.DaysOfWeek[i]))
			}
			sendDays[day] = true
		}

		// until we are in the future, increment a day until we reach a day of week we send on
		for !next.After(now) || !sendDays[next.Weekday()] {
			next = next.AddDate(0, 0, 1)
		}

		return &next, nil

	case RepeatPeriodMonthly:
		if s.s.DayOfMonth == nil {
			return nil, errors.Errorf("schedule %d repeats monthly but has no repeat_day_of_month", s.s.ID)
		}

		// figure out our next fire day, in the case that they asked for a day greater than the number of days
		// in a month, fire on the last day of the month instead
		day := *s.s.DayOfMonth
		maxDay := daysInMonth(next)
		if day > maxDay {
			day = maxDay
		}
		next = time.Date(next.Year(), next.Month(), day, hour, minute, 0, 0, tz)

		// this is in the past, move forward a month
		for !next.After(now) {
			next = time.Date(next.Year(), next.Month()+1, 1, hour, minute, 0, 0, tz)
			day = *s.s.DayOfMonth
			maxDay = daysInMonth(next)
			if day > maxDay {
				day = maxDay
			}
			next = time.Date(next.Year(), next.Month(), day, hour, minute, 0, 0, tz)
		}

		return &next, nil
	default:
		return nil, fmt.Errorf("unknown repeat period: %s", s.s.RepeatPeriod)
	}
}

// returns number of days in the month for the passed in date using crazy golang date magic
func daysInMonth(t time.Time) int {
	// day 0 of a month is previous day of previous month, months can be > 12 and roll years
	lastDay := time.Date(t.Year(), t.Month()+1, 0, 0, 0, 0, 0, t.Location())
	return lastDay.Day()
}

const selectUnfiredSchedules = `
SELECT ROW_TO_JSON(s) FROM (SELECT
	s.id as id,
	s.repeat_hour_of_day as repeat_hour_of_day,
	s.repeat_minute_of_hour as repeat_minute_of_hour,
	s.repeat_day_of_month as repeat_day_of_month,
	s.repeat_days_of_week as repeat_days_of_week,
	s.repeat_period as repeat_period,
	s.next_fire as next_fire,
	s.last_fire as last_fire,
	s.org_id as org_id,
	o.timezone as timezone,
	(SELECT ROW_TO_JSON(sb) FROM (
		SELECT
			b.id as broadcast_id,
			(SELECT JSON_OBJECT_AGG(ts.key, ts.value) FROM (SELECT key, JSON_BUILD_OBJECT('text', t.value) as value FROM each(b.text) t) ts) as translations,
			'unevaluated' as template_state,
			b.base_language as base_language,
			s.org_id as org_id,
			(SELECT ARRAY_AGG(bc.contact_id) FROM (
				SELECT
					bc.contact_id
				FROM
					msgs_broadcast_contacts bc
				WHERE
					bc.broadcast_id = b.id
			) bc) as contact_ids,
			(SELECT ARRAY_AGG(bg.contactgroup_id) FROM (
				SELECT
					bg.contactgroup_id
				FROM
					msgs_broadcast_groups bg
				WHERE
					bg.broadcast_id = b.id
			) bg) as group_ids,
			(SELECT ARRAY_AGG(bu.urn) FROM (
				SELECT
				    cu.identity || '?id=' || cu.id as urn
				FROM
					msgs_broadcast_urns bus JOIN
					contacts_contacturn cu ON cu.id = bus.contacturn_id
				WHERE
					bus.broadcast_id = b.id
			) bu) as urns
		FROM
			msgs_broadcast b
		WHERE
			b.schedule_id = s.id
	) sb) as broadcast,
	(SELECT ROW_TO_JSON(st) FROM (
		SELECT
			t.id as id,
			s.org_id as org_id,
			'T' as start_type,
			t.flow_id as flow_id,
			f.flow_type as flow_type,
			TRUE as restart_participants,
			TRUE as include_active,
			(SELECT ARRAY_AGG(tc.contact_id) FROM (
				SELECT
					tc.contact_id
				FROM
					triggers_trigger_contacts tc
				WHERE
					tc.trigger_id = t.id
			) tc) as contact_ids,
			(SELECT ARRAY_AGG(tg.contactgroup_id) FROM (
				SELECT
					tg.contactgroup_id
				FROM
					triggers_trigger_groups tg
				WHERE
					tg.trigger_id = t.id
			) tg) as group_ids,
			(SELECT ARRAY_AGG(tg.contactgroup_id) FROM (
				SELECT
					tg.contactgroup_id
				FROM
					triggers_trigger_exclude_groups tg
				WHERE
					tg.trigger_id = t.id
			) tg) as exclude_group_ids
		FROM
			triggers_trigger t JOIN
			flows_flow f on t.flow_id = f.id
		WHERE
			t.schedule_id = s.id AND
			t.is_active = TRUE AND
			t.is_archived = FALSE
	) st) as start
FROM
	schedules_schedule s JOIN
	orgs_org o ON s.org_id = o.id
WHERE
	s.is_active = TRUE AND
	s.next_fire < NOW()
ORDER BY
    s.next_fire ASC
) s;
`

// GetUnfiredSchedules returns all unfired schedules
func GetUnfiredSchedules(ctx context.Context, db Queryer) ([]*Schedule, error) {
	rows, err := db.QueryxContext(ctx, selectUnfiredSchedules)
	if err != nil {
		return nil, errors.Wrapf(err, "error selecting unfired schedules")
	}
	defer rows.Close()

	unfired := make([]*Schedule, 0, 10)
	for rows.Next() {
		s := &Schedule{}
		err := dbutil.ScanJSON(rows, &s.s)
		if err != nil {
			return nil, errors.Wrapf(err, "error reading schedule")
		}
		unfired = append(unfired, s)
	}

	return unfired, nil
}

// MarshalJSON marshals into JSON. 0 values will become null
func (i ScheduleID) MarshalJSON() ([]byte, error) {
	return null.Int(i).MarshalJSON()
}

// UnmarshalJSON unmarshals from JSON. null values become 0
func (i *ScheduleID) UnmarshalJSON(b []byte) error {
	return null.UnmarshalInt(b, (*null.Int)(i))
}

// Value returns the db value, null is returned for 0
func (i ScheduleID) Value() (driver.Value, error) {
	return null.Int(i).Value()
}

// Scan scans from the db value. null values become 0
func (i *ScheduleID) Scan(value interface{}) error {
	return null.ScanInt(value, (*null.Int)(i))
}
