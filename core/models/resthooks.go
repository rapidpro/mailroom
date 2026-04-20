package models

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/assets"
	"github.com/pkg/errors"
)

// ResthookID is our type for the database id of a resthook
type ResthookID int

// Resthook is the mailroom type for resthooks
type Resthook struct {
	ID_          ResthookID `json:"id"`
	Slug_        string     `json:"slug"`
	Subscribers_ []string   `json:"subscribers"`
}

// ID returns the ID of this resthook
func (r *Resthook) ID() ResthookID { return r.ID_ }

// Slug returns the slug for this resthook
func (r *Resthook) Slug() string { return r.Slug_ }

// Subscribers returns the subscribers for this resthook
func (r *Resthook) Subscribers() []string { return r.Subscribers_ }

// loads the resthooks for the passed in org
func loadResthooks(ctx context.Context, db *sql.DB, orgID OrgID) ([]assets.Resthook, error) {
	rows, err := db.QueryContext(ctx, sqlSelectResthooksByOrg, orgID)
	if err != nil {
		return nil, errors.Wrapf(err, "error querying resthooks for org: %d", orgID)
	}

	return ScanJSONRows(rows, func() assets.Resthook { return &Resthook{} })
}

const sqlSelectResthooksByOrg = `
SELECT ROW_TO_JSON(r) FROM (
      SELECT id, slug, (
          SELECT ARRAY_AGG(u.target_url) FROM (SELECT rs.target_url FROM api_resthooksubscriber rs WHERE r.id = rs.resthook_id AND rs.is_active = TRUE ORDER BY rs.target_url ASC) u
        ) subscribers
        FROM api_resthook r
       WHERE r.org_id = $1 AND r.is_active = TRUE
    ORDER BY r.slug ASC
) r;`

// UnsubscribeResthooks unsubscribles all the resthooks passed in
func UnsubscribeResthooks(ctx context.Context, tx *sqlx.Tx, unsubs []*ResthookUnsubscribe) error {
	err := BulkQuery(ctx, "unsubscribing resthooks", tx, sqlUnsubscribeResthooks, unsubs)
	return errors.Wrapf(err, "error unsubscribing from resthooks")
}

type ResthookUnsubscribe struct {
	OrgID OrgID  `db:"org_id"`
	Slug  string `db:"slug"`
	URL   string `db:"url"`
}

const sqlUnsubscribeResthooks = `
UPDATE api_resthooksubscriber
   SET is_active = FALSE, modified_on = NOW()
 WHERE id = ANY(
    SELECT s.id 
      FROM api_resthooksubscriber s
      JOIN api_resthook r ON s.resthook_id = r.id, (VALUES(:org_id, :slug, :url)) AS u(org_id, slug, url)
     WHERE s.is_active = TRUE AND r.org_id = u.org_id::int AND r.slug = u.slug AND s.target_url = u.url
)`
