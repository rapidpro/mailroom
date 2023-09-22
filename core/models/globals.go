package models

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/nyaruka/gocommon/dbutil"
	"github.com/nyaruka/goflow/assets"
	"github.com/pkg/errors"
)

type Global struct {
	g struct {
		Key   string `json:"key"   validate:"required"`
		Name  string `json:"name"  validate:"required"`
		Value string `json:"value"`
	}
}

func (g *Global) Key() string   { return g.g.Key }
func (g *Global) Name() string  { return g.g.Name }
func (g *Global) Value() string { return g.g.Value }

// UnmarshalJSON is our unmarshaller for json data
func (g *Global) UnmarshalJSON(data []byte) error { return json.Unmarshal(data, &g.g) }

// MarshalJSON is our marshaller for json data
func (g *Global) MarshalJSON() ([]byte, error) { return json.Marshal(g.g) }

// loads the globals for the passed in org
func loadGlobals(ctx context.Context, db *sql.DB, orgID OrgID) ([]assets.Global, error) {
	rows, err := db.QueryContext(ctx, selectGlobalsSQL, orgID)
	if err != nil {
		return nil, errors.Wrapf(err, "error querying globals for org: %d", orgID)
	}
	defer rows.Close()

	globals := make([]assets.Global, 0)
	for rows.Next() {
		global := &Global{}
		err = dbutil.ScanAndValidateJSON(rows, &global.g)
		if err != nil {
			return nil, errors.Wrap(err, "error reading global row")
		}

		globals = append(globals, global)
	}

	return globals, nil
}

const selectGlobalsSQL = `
SELECT ROW_TO_JSON(r) FROM (SELECT
	g.key as key,
	g.name as name, 
	g.value as value
FROM 
	globals_global g
WHERE 
	org_id = $1 AND
	is_active = TRUE
ORDER BY 
	key ASC
) r;
`
