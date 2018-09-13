package models

import (
	"context"

	"github.com/jmoiron/sqlx"
	"github.com/juju/errors"
	"github.com/nyaruka/goflow/assets"
)

type GroupID int

// Group is our mailroom type for contact groups
type Group struct {
	g struct {
		ID    GroupID          `json:"id"`
		UUID  assets.GroupUUID `json:"uuid"`
		Name  string           `json:"name"`
		Query string           `json:"query"`
	}
}

// ID returns the ID for this group
func (g *Group) ID() GroupID { return g.g.ID }

// UUID returns the uuid for this group
func (g *Group) UUID() assets.GroupUUID { return g.g.UUID }

// Name returns the name for this group
func (g *Group) Name() string { return g.g.Name }

// Query returns the query string (if any) for this group
func (g *Group) Query() string { return g.g.Query }

// loads the groups for the passed in org
func loadGroups(ctx context.Context, db sqlx.Queryer, orgID OrgID) ([]assets.Group, error) {
	rows, err := db.Queryx(selectGroupsSQL, orgID)
	if err != nil {
		return nil, errors.Annotatef(err, "error querying groups for org: %d", orgID)
	}
	defer rows.Close()

	groups := make([]assets.Group, 0, 10)
	for rows.Next() {
		group := &Group{}
		err = readJSONRow(rows, &group.g)
		if err != nil {
			return nil, errors.Annotate(err, "error reading group row")
		}

		groups = append(groups, group)
	}

	return groups, nil
}

const selectGroupsSQL = `
SELECT ROW_TO_JSON(r) FROM (SELECT
	id, 
	uuid, 
	name, 
	query
FROM 
	contacts_contactgroup 
WHERE 
	org_id = $1 AND 
	is_active = TRUE AND
	group_type = 'U'
ORDER BY 
	name ASC
) r;
`
