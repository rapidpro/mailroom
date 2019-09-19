package models

import (
	"context"
	"database/sql/driver"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/null"
	"github.com/pkg/errors"
)

// ScheduleID is our internal type for schedule IDs
type ScheduleID null.Int

// NilScheduleID is our constant for a nil schedule id
const NilScheduleID = ScheduleID(0)

type RepeatPeriod string

const RepeatPeriodNever = "O"
const RepeatPeriodDaily = "D"
const RepeatPeriodWeekly = "W"
const RepeatPeriodMonthly = "M"

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
		ID ScheduleID `json:"id"                    db:"id"`

		RepeatPeriod RepeatPeriod `json:"repeat_period"         db:"repeat_period"`

		HourOfDay    *int        `json:"repeat_hour_of_day"    db:"repeat_hour_of_day"`
		MinuteOfHour *int        `json:"repeat_minute_of_hour" db:"repeat_minute_of_hour"`
		DayOfMonth   *int        `json:"repeat_day_of_month"   db:"repeat_day_of_month"`
		DaysOfWeek   null.String `json:"repeat_days_of_week"   db:"repeat_days_of_week"`

		NextFire *time.Time `json:"next_fire"             db:"next_fire"`
		LastFire *time.Time `json:"last_fire"             db:"last_fire"`

		OrgID OrgID `json:"org_id"                db:"org_id"`
	}
}

func (s *Schedule) ID() ScheduleID { return s.s.ID }

// GetNextFire returns the next fire for this schedule (if any)
func (s *Schedule) GetNextFire(tz *time.Location, now time.Time, start time.Time) (*time.Time, error) {
	// should have hour and minute on everything else
	if s.s.HourOfDay == nil {
		return nil, errors.Errorf("schedule %d has no repeat_hour_of_day set", s.s.ID)
	}
	if s.s.MinuteOfHour == nil {
		return nil, errors.Errorf("schedule %d has no repeat_minute_of_hour set", s.s.ID)
	}

	// change our time to be in our location
	start = start.In(tz)
	minute := *s.s.MinuteOfHour
	hour := *s.s.HourOfDay

	// set our next fire to today at the specified hour and minute
	next := time.Date(start.Year(), start.Month(), start.Day(), hour, minute, 0, 0, tz)

	switch s.s.RepeatPeriod {

	case RepeatPeriodNever:
		if !next.After(now) {
			return nil, nil
		}
		return &next, nil

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
	SELECT ROW_TO_JSON(r) FROM (SELECT 
		id,
		repeat_hour_of_day,
		repeat_minute_of_hour,
		repeat_day_of_month,
		repeat_days,
		repeat_period,
		next_fire,
		last_fire
	FROM
		schedules_schedule s
	WHERE
		s.is_active = TRUE AND	
		s.next_fire < NOW()
	) r;
`

// GetUnfiredSchedules returns all unfired schedules
func GetUnfiredSchedules(ctx context.Context, db *sqlx.DB) ([]*Schedule, error) {
	rows, err := db.QueryxContext(ctx, selectUnfiredSchedules)
	if err != nil {
		return nil, errors.Wrapf(err, "error selecting unfired schedules")
	}
	defer rows.Close()

	unfired := make([]*Schedule, 0, 10)
	for rows.Next() {
		s := &Schedule{}
		err := readJSONRow(rows, s.s)
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
