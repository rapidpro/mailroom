package models

import (
	"context"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/assets"
	"github.com/pkg/errors"
)

type LabelID int

// Label is our mailroom type for message labels
type Label struct {
	l struct {
		ID   LabelID          `json:"id"`
		UUID assets.LabelUUID `json:"uuid"`
		Name string           `json:"name"`
	}
}

// ID returns the ID for this label
func (l *Label) ID() LabelID { return l.l.ID }

// UUID returns the uuid for this label
func (l *Label) UUID() assets.LabelUUID { return l.l.UUID }

// Name returns the name for this label
func (l *Label) Name() string { return l.l.Name }

// loads the labels for the passed in org
func loadLabels(ctx context.Context, db sqlx.Queryer, orgID OrgID) ([]assets.Label, error) {
	rows, err := db.Queryx(selectLabelsSQL, orgID)
	if err != nil {
		return nil, errors.Wrapf(err, "error querying labels for org: %d", orgID)
	}
	defer rows.Close()

	labels := make([]assets.Label, 0, 10)
	for rows.Next() {
		label := &Label{}
		err = readJSONRow(rows, &label.l)
		if err != nil {
			return nil, errors.Wrap(err, "error scanning label row")
		}
		labels = append(labels, label)
	}

	return labels, nil
}

const selectLabelsSQL = `
SELECT ROW_TO_JSON(r) FROM (SELECT
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
) r;
`
