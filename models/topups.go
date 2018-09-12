package models

import (
	"context"

	"github.com/jmoiron/sqlx"
	"github.com/juju/errors"
)

// TODO: cache this in redis?
// loadActiveTopup loads the active topup for the passed in org
func loadActiveTopup(ctx context.Context, db sqlx.Queryer, orgID OrgID) (TopUpID, error) {
	var topupID TopUpID
	rows, err := db.Queryx(selectActiveTopup, orgID)
	if err != nil {
		return topupID, errors.Annotatef(err, "error loading active topup for org: %d", orgID)
	}
	defer rows.Close()

	if rows.Next() {
		err = rows.Scan(&topupID)
		if err != nil {
			return topupID, errors.Annotatef(err, "error scanning topup id")
		}
	}

	return topupID, nil
}

const selectActiveTopup = `
SELECT 
	t.id as id
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
	SUM(tc.used) < (t.credits) OR 
	SUM(tc.used) IS NULL 
ORDER BY 
	t.expires_on ASC, t.id ASC
LIMIT 1
`
