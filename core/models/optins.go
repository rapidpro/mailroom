package models

import (
	"context"
	"database/sql"
	"database/sql/driver"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/null/v3"
	"github.com/pkg/errors"
)

// OptInID is our type for the database id of an optin
type OptInID int

const NilOptInID = OptInID(0)

func (i *OptInID) Scan(value any) error         { return null.ScanInt(value, i) }
func (i OptInID) Value() (driver.Value, error)  { return null.IntValue(i) }
func (i *OptInID) UnmarshalJSON(b []byte) error { return null.UnmarshalInt(b, i) }
func (i OptInID) MarshalJSON() ([]byte, error)  { return null.MarshalInt(i) }

// OptIn is the mailroom type for optins
type OptIn struct {
	ID_   OptInID          `json:"id"`
	UUID_ assets.OptInUUID `json:"uuid"`
	Name_ string           `json:"name"`
}

func (o *OptIn) ID() OptInID            { return o.ID_ }
func (o *OptIn) UUID() assets.OptInUUID { return o.UUID_ }
func (o *OptIn) Name() string           { return o.Name_ }

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

	return ScanJSONRows(rows, func() assets.OptIn { return &OptIn{} })
}
