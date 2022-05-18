package models

import (
	"context"
	"fmt"
	"time"

	"github.com/nyaruka/gocommon/dates"
)

type scopedCount struct {
	CountType string `db:"count_type"`
	Scope     string `db:"scope"`
	Count     int    `db:"count"`
}

type dailyCount struct {
	scopedCount

	Day dates.Date `db:"day"`
}

const sqlInsertDailyCount = `INSERT INTO %s(count_type, scope, day, count, is_squashed) VALUES(:count_type, :scope, :day, :count, FALSE)`

func insertDailyCounts(ctx context.Context, tx Queryer, table string, countType TicketDailyCountType, tz *time.Location, scopeCounts map[string]int) error {
	day := dates.ExtractDate(dates.Now().In(tz))

	counts := make([]*dailyCount, 0, len(scopeCounts))
	for scope, count := range scopeCounts {
		counts = append(counts, &dailyCount{
			scopedCount: scopedCount{
				CountType: string(countType),
				Scope:     scope,
				Count:     count,
			},
			Day: day,
		})
	}

	return BulkQuery(ctx, "inserted daily counts", tx, fmt.Sprintf(sqlInsertDailyCount, table), counts)
}

type dailyTiming struct {
	dailyCount

	Seconds int64 `db:"seconds"`
}

const sqlInsertDailyTiming = `INSERT INTO %s(count_type, scope, day, count, seconds, is_squashed) VALUES(:count_type, :scope, :day, :count, :seconds, FALSE)`

func insertDailyTiming(ctx context.Context, tx Queryer, table string, countType TicketDailyTimingType, tz *time.Location, scope string, duration time.Duration) error {
	day := dates.ExtractDate(dates.Now().In(tz))
	timing := &dailyTiming{
		dailyCount: dailyCount{
			scopedCount: scopedCount{
				CountType: string(countType),
				Scope:     scope,
				Count:     1,
			},
			Day: day,
		},
		Seconds: int64(duration / time.Second),
	}

	_, err := tx.NamedExecContext(ctx, fmt.Sprintf(sqlInsertDailyTiming, table), timing)
	return err
}

func scopeOrg(oa *OrgAssets) string {
	return fmt.Sprintf("o:%d", oa.OrgID())
}

func scopeTeam(t *Team) string {
	return fmt.Sprintf("t:%d", t.ID)
}

func scopeUser(oa *OrgAssets, u *User) string {
	return fmt.Sprintf("o:%d:u:%d", oa.OrgID(), u.ID())
}
