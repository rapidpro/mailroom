package models

import (
	"context"
	"database/sql"

	"github.com/nyaruka/goflow/assets"
	"github.com/pkg/errors"
)

type Global struct {
	Key_   string `json:"key"`
	Name_  string `json:"name"`
	Value_ string `json:"value"`
}

func (g *Global) Key() string   { return g.Key_ }
func (g *Global) Name() string  { return g.Name_ }
func (g *Global) Value() string { return g.Value_ }

// loads the globals for the passed in org
func loadGlobals(ctx context.Context, db *sql.DB, orgID OrgID) ([]assets.Global, error) {
	rows, err := db.QueryContext(ctx, sqlSelectGlobalsByOrg, orgID)
	if err != nil {
		return nil, errors.Wrapf(err, "error querying globals for org: %d", orgID)
	}

	return ScanJSONRows(rows, func() assets.Global { return &Global{} })
}

const sqlSelectGlobalsByOrg = `
SELECT ROW_TO_JSON(r) FROM (
      SELECT g.key as key, g.name as name, g.value as value
        FROM globals_global g
       WHERE org_id = $1 AND is_active = TRUE
    ORDER BY key ASC
) r;`
