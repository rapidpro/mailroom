package models

import (
	"context"
	"time"

	"github.com/nyaruka/gocommon/dbutil"
	"github.com/nyaruka/goflow/assets"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
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
	start := time.Now()

	rows, err := db.Queryx(selectResthooksSQL, orgID)
	if err != nil {
		return nil, errors.Wrapf(err, "error querying resthooks for org: %d", orgID)
	}
	defer rows.Close()

	resthooks := make([]assets.Resthook, 0, 10)
	for rows.Next() {
		resthook := &Resthook{}
		err = dbutil.ScanJSON(rows, &resthook.r)
		if err != nil {
			return nil, errors.Wrap(err, "error scanning resthook row")
		}

		resthooks = append(resthooks, resthook)
	}

	logrus.WithField("elapsed", time.Since(start)).WithField("org_id", orgID).WithField("count", len(resthooks)).Debug("loaded resthooks")

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
) r;
`

// UnsubscribeResthooks unsubscribles all the resthooks passed in
func UnsubscribeResthooks(ctx context.Context, tx *sqlx.Tx, unsubs []*ResthookUnsubscribe) error {
	is := make([]interface{}, len(unsubs))
	for i := range unsubs {
		is[i] = unsubs[i]
	}

	err := BulkQuery(ctx, "unsubscribing resthooks", tx, unsubscribeResthooksSQL, is)
	if err != nil {
		return errors.Wrapf(err, "error unsubscribing from resthooks")
	}

	return nil
}

type ResthookUnsubscribe struct {
	OrgID OrgID  `db:"org_id"`
	Slug  string `db:"slug"`
	URL   string `db:"url"`
}

const unsubscribeResthooksSQL = `
UPDATE 
	api_resthooksubscriber
SET 
	is_active = FALSE, 
	modified_on = NOW()
WHERE
	id = ANY(
		SELECT 
			s.id 
		FROM 
			api_resthooksubscriber s
			JOIN api_resthook r ON s.resthook_id = r.id,
			(VALUES(:org_id, :slug, :url)) AS u(org_id, slug, url)
		WHERE 
			s.is_active = TRUE AND
			r.org_id = u.org_id::int AND
			r.slug = u.slug AND
			s.target_url = u.url
	)
`
