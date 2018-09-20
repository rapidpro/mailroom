package models

import (
	"context"
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/juju/errors"
	"github.com/sirupsen/logrus"
	null "gopkg.in/guregu/null.v3"
)

// TopupID is our type for topup ids, which can be null
type TopupID null.Int

// NilTopupID is our nil value for topup id
var NilTopupID = TopupID(null.NewInt(0, false))

const (
	// also check lua script if modifying these
	redisOrgCredistsUsedKey  = `org:%d:cache:credits_used`
	redisActiveTopupKey      = `org:%d:cache:active_topup`
	redisCreditsRemainingKey = `org:%d:cache:credits_remaining:%d`
)

func decrementOrgCredits(ctx context.Context, db sqlx.Queryer, rc redis.Conn, orgID OrgID, amount int) (TopupID, error) {
	// no matter what we decrement our org credit
	topups, err := redis.Ints(decrementCreditLua.Do(rc, orgID, amount))
	if err != nil {
		return NilTopupID, err
	}

	// we found an active topup, return it
	if topups[0] > 0 {
		return TopupID(null.NewInt(int64(topups[0]), true)), err
	}

	// no active topup found, lets calculate it
	topup, err := calculateActiveTopup(ctx, db, orgID)
	if err != nil {
		return NilTopupID, err
	}

	// no topup found, oh well
	if topup == nil {
		return NilTopupID, nil
	}

	// got one? then cache it
	expireSeconds := -int(time.Since(topup.Expiration) / time.Second)
	if expireSeconds > 0 && topup.Remaining-amount > 0 {
		rc.Send("SETEX", fmt.Sprintf(redisActiveTopupKey, orgID), expireSeconds, topup.ID.Int64)
		_, err := rc.Do("SETEX", fmt.Sprintf(redisCreditsRemainingKey, orgID, topup.ID.Int64), expireSeconds, topup.Remaining-amount)
		if err != nil {
			// an error here isn't the end of the world, log it and move on
			logrus.WithError(err).Errorf("error setting active topup in redis for org: %d", orgID)
		}
	}

	return topup.ID, nil
}

var decrementCreditLua = redis.NewScript(2, `-- KEYS: [OrgID] [Amount]
-- first check whether we have an org level cache of credits used, and if so decrement it
local ttl = redis.call('ttl', 'org:' .. KEYS[1] .. ':cache:credits_used')
if ttl > 0 then
    redis.call('incrby', 'org:' .. KEYS[1] .. ':cache:credits_used', KEYS[2])
end

-- look up our active topup
local orgKey = 'org:' .. KEYS[1] .. ':cache:active_topup'
local activeTopup = redis.call('get', orgKey)
local remaining = -1

-- found an active topup, try do decrement its credits
if activeTopup then
    local topupKey = 'org:' .. KEYS[1] .. ':cache:credits_remaining:' .. tonumber(activeTopup)
	remaining = redis.call('decrby', topupKey, KEYS[2])
	if remaining <= 0 then
		redis.call('del', topupKey, orgKey)
	end
-- no active topup cached
else
	activeTopup = -1
end

return {activeTopup, remaining}
`)

// calculateActiveTopup loads the active topup for the passed in org
func calculateActiveTopup(ctx context.Context, db sqlx.Queryer, orgID OrgID) (*Topup, error) {
	topup := &Topup{}
	rows, err := db.Queryx(selectActiveTopup, orgID)
	if err != nil {
		return nil, errors.Annotatef(err, "error loading active topup for org: %d", orgID)
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, nil
	}

	err = rows.StructScan(topup)
	if err != nil {
		return nil, errors.Annotatef(err, "error scanning topup")
	}

	return topup, nil
}

// Topup is our internal struct representing an org's topup and expiration date
type Topup struct {
	ID         TopupID   `db:"id"`
	Remaining  int       `db:"remaining"`
	Expiration time.Time `db:"expires_on"`
}

const selectActiveTopup = `
SELECT 
	t.id as id,
	t.credits - COALESCE(SUM(tc.used), 0) as remaining,
	t.expires_on as expires_on
FROM 
	orgs_topup t
	LEFT OUTER JOIN orgs_topupcredits tc ON (t.id = tc.topup_id) 
WHERE 
	t.org_id = $1 AND
	t.expires_on >= NOW() AND
	t.is_active = TRUE AND
	t.credits > 0
GROUP BY 
	t.id 
HAVING 
	COALESCE(SUM(tc.used), 0) < (t.credits)
ORDER BY 
	t.expires_on ASC, t.id ASC
LIMIT 1
`
