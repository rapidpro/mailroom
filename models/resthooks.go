package models

import (
	"context"

	"github.com/jmoiron/sqlx"
	"github.com/juju/errors"
	"github.com/lib/pq"
	"github.com/nyaruka/goflow/assets"
)

// ResthookID is our type for the database id of a resthook
type ResthookID int64

// Resthook is the mailroom type for resthooks
type Resthook struct {
	id          ResthookID
	slug        string
	subscribers []string
}

// ID returns the ID of this resthook
func (r *Resthook) ID() ResthookID { return r.id }

// Slug returns the slug for this resthook
func (r *Resthook) Slug() string { return r.slug }

// Subscribers returns the subscribers for this resthook
func (r *Resthook) Subscribers() []string { return r.subscribers }

// loads the resthooks for the passed in org
func loadResthooks(ctx context.Context, db sqlx.Queryer, orgID OrgID) ([]assets.Resthook, error) {
	rows, err := db.Query(selectResthooksSQL, orgID)
	if err != nil {
		return nil, errors.Annotatef(err, "error querying resthooks for org: %d", orgID)
	}
	defer rows.Close()

	resthooks := make([]assets.Resthook, 0, 10)
	for rows.Next() {
		resthook := &Resthook{}

		err := rows.Scan(&resthook.id, &resthook.slug, pq.Array(&resthook.subscribers))
		if err != nil {
			return nil, errors.Annotate(err, "error scanning resthook row")
		}

		resthooks = append(resthooks, resthook)
	}

	return resthooks, nil
}

const selectResthooksSQL = `
SELECT
	id,
	slug,
	(SELECT ARRAY_AGG(u.target_url) FROM (
		SELECT
			rs.target_url
		FROM
			api_resthooksubscriber rs 
		WHERE
			r.id = rs.resthook_id AND 
			rs.is_active = TRUE
		ORDER BY
			rs.target_url ASC
	) u) subscribers
FROM 
	api_resthook r
WHERE 
	r.org_id = $1 AND 
	r.is_active = TRUE
ORDER BY
	r.slug ASC`
