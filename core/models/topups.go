package models

import (
	"context"
	"database/sql/driver"
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/nyaruka/null"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// TopupID is our type for topup ids, which can be null
type TopupID null.Int

// NilTopupID is our nil value for topup id
var NilTopupID = TopupID(0)

const (
	// also check lua script if modifying these
	redisOrgCredistsUsedKey  = `org:%d:cache:credits_used`
	redisActiveTopupKey      = `org:%d:cache:active_topup`
	redisCreditsRemainingKey = `org:%d:cache:credits_remaining:%d`
)

// AllocateTopups allocates topups for the given number of messages if topups are used by the org.
// If topups are allocated it will return the ID of the topup to assign to those messages.
func AllocateTopups(ctx context.Context, db Queryer, rp *redis.Pool, org *Org, amount int) (TopupID, error) {
	rc := rp.Get()
	defer rc.Close()

	// if org doesn't use topups, do nothing
	if !org.UsesTopups() {
		return NilTopupID, nil
	}

	// no matter what we decrement our org credit
	topups, err := redis.Ints(decrementCreditLua.Do(rc, org.ID(), amount))
	if err != nil {
		return NilTopupID, err
	}

	// we found an active topup, return it
	if topups[0] > 0 {
		return TopupID(topups[0]), nil
	}

	// no active topup found, lets calculate it
	topup, err := calculateActiveTopup(ctx, db, org.ID())
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
		rc.Send("SETEX", fmt.Sprintf(redisActiveTopupKey, org.ID()), expireSeconds, topup.ID)
		_, err := rc.Do("SETEX", fmt.Sprintf(redisCreditsRemainingKey, org.ID(), topup.ID), expireSeconds, topup.Remaining-amount)
		if err != nil {
			// an error here isn't the end of the world, log it and move on
			logrus.WithError(err).Errorf("error setting active topup in redis for org: %d", org.ID())
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
func calculateActiveTopup(ctx context.Context, db Queryer, orgID OrgID) (*Topup, error) {
	topup := &Topup{}
	rows, err := db.QueryxContext(ctx, selectActiveTopup, orgID)
	if err != nil {
		return nil, errors.Wrapf(err, "error loading active topup for org: %d", orgID)
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, nil
	}

	err = rows.StructScan(topup)
	if err != nil {
		return nil, errors.Wrapf(err, "error scanning topup")
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

// MarshalJSON marshals into JSON. 0 values will become null
func (i TopupID) MarshalJSON() ([]byte, error) {
	return null.Int(i).MarshalJSON()
}

// UnmarshalJSON unmarshals from JSON. null values become 0
func (i *TopupID) UnmarshalJSON(b []byte) error {
	return null.UnmarshalInt(b, (*null.Int)(i))
}

// Value returns the db value, null is returned for 0
func (i TopupID) Value() (driver.Value, error) {
	return null.Int(i).Value()
}

// Scan scans from the db value. null values become 0
func (i *TopupID) Scan(value interface{}) error {
	return null.ScanInt(value, (*null.Int)(i))
}
