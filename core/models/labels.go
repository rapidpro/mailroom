package models

import (
	"context"
	"time"

	"github.com/nyaruka/gocommon/dbutil"
	"github.com/nyaruka/goflow/assets"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
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
	start := time.Now()

	rows, err := db.Queryx(selectLabelsSQL, orgID)
	if err != nil {
		return nil, errors.Wrapf(err, "error querying labels for org: %d", orgID)
	}
	defer rows.Close()

	labels := make([]assets.Label, 0, 10)
	for rows.Next() {
		label := &Label{}
		err = dbutil.ScanJSON(rows, &label.l)
		if err != nil {
			return nil, errors.Wrap(err, "error scanning label row")
		}
		labels = append(labels, label)
	}

	logrus.WithField("elapsed", time.Since(start)).WithField("org_id", orgID).WithField("count", len(labels)).Debug("loaded labels")

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

// AddMsgLabels inserts the passed in msg labels to our db
func AddMsgLabels(ctx context.Context, tx *sqlx.Tx, adds []*MsgLabelAdd) error {
	err := BulkQuery(ctx, "inserting msg labels", tx, insertMsgLabelsSQL, adds)
	return errors.Wrapf(err, "error inserting new msg labels")
}

const insertMsgLabelsSQL = `
INSERT INTO 
	msgs_msg_labels(msg_id, label_id)
	VALUES(:msg_id, :label_id)
ON CONFLICT
	DO NOTHING
`

// MsgLabelAdd represents a single label that should be added to a message
type MsgLabelAdd struct {
	MsgID   MsgID   `db:"msg_id"`
	LabelID LabelID `db:"label_id"`
}
