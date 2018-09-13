package models

import (
	"context"

	"github.com/jmoiron/sqlx"
	"github.com/juju/errors"
	"github.com/nyaruka/goflow/assets"
)

// ResthookID is our type for the database id of a resthook
type ResthookID int64

// Resthook is the mailroom type for resthooks
type Resthook struct {
	r struct {
		ID          ResthookID `json:"id"`
		Slug        string     `json:"slug"`
		Subscribers []string   `json:"subscribers"`
	}
}

// ID returns the ID of this resthook
func (r *Resthook) ID() ResthookID { return r.r.ID }

// Slug returns the slug for this resthook
func (r *Resthook) Slug() string { return r.r.Slug }

// Subscribers returns the subscribers for this resthook
func (r *Resthook) Subscribers() []string { return r.r.Subscribers }

// loads the resthooks for the passed in org
func loadResthooks(ctx context.Context, db sqlx.Queryer, orgID OrgID) ([]assets.Resthook, error) {
	rows, err := db.Queryx(selectResthooksSQL, orgID)
	if err != nil {
		return nil, errors.Annotatef(err, "error querying resthooks for org: %d", orgID)
	}
	defer rows.Close()

	resthooks := make([]assets.Resthook, 0, 10)
	for rows.Next() {
		resthook := &Resthook{}
		err = readJSONRow(rows, &resthook.r)
		if err != nil {
			return nil, errors.Annotate(err, "error scanning resthook row")
		}

		resthooks = append(resthooks, resthook)
	}

	return resthooks, nil
}

const selectResthooksSQL = `
SELECT ROW_TO_JSON(r) FROM (SELECT
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
	r.slug ASC
) r;`
