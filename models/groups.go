package models

import (
	"context"

	"github.com/jmoiron/sqlx"
	"github.com/juju/errors"
	"github.com/nyaruka/goflow/flows"
	null "gopkg.in/guregu/null.v3"
)

// Group is our mailroom type for contact groups
type Group struct {
	id    flows.GroupID
	uuid  flows.GroupUUID
	name  string
	query string
}

// ID returns the ID for this group
func (g *Group) ID() flows.GroupID { return g.id }

// UUID returns the uuid for this group
func (g *Group) UUID() flows.GroupUUID { return g.uuid }

// Name returns the name for this group
func (g *Group) Name() string { return g.name }

// Query returns the query string (if any) for this group
func (g *Group) Query() string { return g.query }

// loads the groups for the passed in org
func loadGroups(ctx context.Context, db sqlx.Queryer, orgID OrgID) ([]*Group, error) {
	rows, err := db.Query(selectGroupsSQL, orgID)
	if err != nil {
		return nil, errors.Annotatef(err, "error querying groups for org: %d", orgID)
	}
	defer rows.Close()

	groups := make([]*Group, 0, 10)
	for rows.Next() {
		group := &Group{}
		query := null.String{}

		err := rows.Scan(&group.id, &group.uuid, &group.name, &query)
		if err != nil {
			return nil, errors.Annotate(err, "error scanning group row")
		}
		group.query = query.String

		groups = append(groups, group)
	}

	return groups, nil
}

const selectGroupsSQL = `
SELECT
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
`
