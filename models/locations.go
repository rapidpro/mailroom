package models

import (
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/juju/errors"
	"github.com/lib/pq"
)

// Location is our mailroom type for administrative locations
type Location struct {
	id       int
	level    int
	parentID *int
	osmID    string
	name     string
	aliases  []string
	children []*Location
}

// ID returns the database id for this location
func (l *Location) ID() int { return l.id }

// Level returns the level for this location
func (l *Location) Level() int { return l.level }

// OSMID returns the OSM ID for this location
func (l *Location) OSMID() string { return l.osmID }

// Name returns the name for this location
func (l *Location) Name() string { return l.name }

// Aliases returns the list of aliases for this location
func (l *Location) Aliases() []string { return l.aliases }

// Children returns the list of children for this location
func (l *Location) Children() []*Location { return l.children }

// loadLocations loads all the locations for this org returning the root node
func loadLocations(ctx context.Context, db sqlx.Queryer, orgID OrgID) (*Location, error) {
	rows, err := db.Query(loadLocationsSQL, orgID)
	if err != nil {
		return nil, errors.Annotatef(err, "error querying locations for org: %d", orgID)
	}
	defer rows.Close()

	// we first read in all our locations into a map by id
	locationMap := make(map[int]*Location)
	locations := make([]*Location, 0, 10)
	var root *Location
	for rows.Next() {
		location := &Location{}

		err := rows.Scan(&location.id, &location.level, &location.osmID, &location.parentID, &location.name, pq.Array(&location.aliases))
		if err != nil {
			return nil, errors.Annotate(err, "error scanning location row")
		}
		locationMap[location.id] = location
		locations = append(locations, location)

		if location.parentID == nil {
			root = location
		}
	}

	// now we make another pass and associate all children
	for _, l := range locations {
		if l.parentID != nil {
			parent, found := locationMap[*l.parentID]
			if !found {
				return nil, fmt.Errorf("unable to find parent: %d for location: %d", *l.parentID, l.id)
			}
			parent.children = append(parent.children, l)
		}
	}

	return root, nil
}

// TODO: this is a bit bananas
const loadLocationsSQL = `
SELECT
	l.id, 
	l.level,	
	l.osm_id, 
	l.parent_id, 
	l.name,
	(SELECT ARRAY_AGG(a.name) FROM (
		SELECT 
			DISTINCT(a.name)
		FROM 
			locations_boundaryalias a
		WHERE 
			a.boundary_id = l.id AND
			a.is_active = TRUE AND
			a.org_id = $1
		ORDER BY 
			a.name
	)a ) aliases
FROM
	locations_adminboundary l
WHERE
	l.id >= (select lft from locations_adminboundary la, orgs_org o where la.id = o.country_id and o.id = $1) and 
	l.id <= (select rght from locations_adminboundary la, orgs_org o where la.id = o.country_id and o.id = $1) and
	l.tree_id = (select tree_id from locations_adminboundary la, orgs_org o where la.id = o.country_id and o.id = $1)
ORDER BY
	l.level, l.id;
`
