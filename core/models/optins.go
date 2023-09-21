package models

import (
	"context"
	"database/sql"

	"github.com/nyaruka/gocommon/dbutil"
	"github.com/nyaruka/goflow/assets"
	"github.com/pkg/errors"
)

// OptInID is our type for the database id of an optin
type OptInID int

const NilOptInID = OptInID(0)

// OptIn is the mailroom type for optins
type OptIn struct {
	o struct {
		ID   OptInID          `json:"id"`
		UUID assets.OptInUUID `json:"uuid"`
		Name string           `json:"name"`
	}
}

func (o *OptIn) ID() OptInID            { return o.o.ID }
func (o *OptIn) UUID() assets.OptInUUID { return o.o.UUID }
func (o *OptIn) Name() string           { return o.o.Name }

func (o *OptIn) Reference() *assets.OptInReference {
	return assets.NewOptInReference(o.o.UUID, o.o.Name)
}

const sqlSelectOptInsByOrg = `
SELECT ROW_TO_JSON(r) FROM (
    SELECT id, uuid, name
      FROM msgs_optin o
     WHERE o.org_id = $1 AND o.is_active
  ORDER BY o.id ASC
) r;`

// loads the optins for the passed in org
func loadOptIns(ctx context.Context, db *sql.DB, orgID OrgID) ([]assets.OptIn, error) {
	rows, err := db.QueryContext(ctx, sqlSelectOptInsByOrg, orgID)
	if err != nil {
		return nil, errors.Wrapf(err, "error querying optins for org: %d", orgID)
	}
	defer rows.Close()

	optIns := make([]assets.OptIn, 0, 10)
	for rows.Next() {
		optIn := &OptIn{}
		err = dbutil.ScanJSON(rows, &optIn.o)
		if err != nil {
			return nil, errors.Wrap(err, "error scanning optin row")
		}

		optIns = append(optIns, optIn)
	}

	return optIns, nil
}
