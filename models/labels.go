package models

import (
	"context"

	"github.com/jmoiron/sqlx"
	"github.com/juju/errors"
	"github.com/nyaruka/goflow/flows"
)

// Label is our mailroom type for message labels
type Label struct {
	id   flows.LabelID
	uuid flows.LabelUUID
	name string
}

// ID returns the ID for this label
func (l *Label) ID() flows.LabelID { return l.id }

// UUID returns the uuid for this label
func (l *Label) UUID() flows.LabelUUID { return l.uuid }

// Name returns the name for this label
func (l *Label) Name() string { return l.name }

// loads the labels for the passed in org
func loadLabels(ctx context.Context, db sqlx.Queryer, orgID OrgID) ([]*Label, error) {
	rows, err := db.Query(selectLabelsSQL, orgID)
	if err != nil {
		return nil, errors.Annotatef(err, "error querying labels for org: %d", orgID)
	}
	defer rows.Close()

	labels := make([]*Label, 0, 10)
	for rows.Next() {
		label := &Label{}
		err := rows.Scan(&label.id, &label.uuid, &label.name)
		if err != nil {
			return nil, errors.Annotate(err, "error scanning label row")
		}
		labels = append(labels, label)
	}

	return labels, nil
}

const selectLabelsSQL = `
SELECT
	id, 
	uuid, 
	name
FROM 
	msgs_label
WHERE 
	org_id = $1 AND 
	is_active = TRUE AND
	label_type = 'L'
ORDER BY 
	name ASC
`
