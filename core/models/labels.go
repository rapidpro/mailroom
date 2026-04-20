package models

import (
	"context"
	"database/sql"

	"github.com/nyaruka/goflow/assets"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

type LabelID int

// Label is our mailroom type for message labels
type Label struct {
	ID_   LabelID          `json:"id"`
	UUID_ assets.LabelUUID `json:"uuid"`
	Name_ string           `json:"name"`
}

// ID returns the ID for this label
func (l *Label) ID() LabelID { return l.ID_ }

// UUID returns the uuid for this label
func (l *Label) UUID() assets.LabelUUID { return l.UUID_ }

// Name returns the name for this label
func (l *Label) Name() string { return l.Name_ }

// loads the labels for the passed in org
func loadLabels(ctx context.Context, db *sql.DB, orgID OrgID) ([]assets.Label, error) {
	rows, err := db.QueryContext(ctx, sqlSelectLabelsByOrg, orgID)
	if err != nil {
		return nil, errors.Wrapf(err, "error querying labels for org: %d", orgID)
	}

	return ScanJSONRows(rows, func() assets.Label { return &Label{} })
}

const sqlSelectLabelsByOrg = `
SELECT ROW_TO_JSON(r) FROM (
      SELECT id, uuid, name
        FROM msgs_label
       WHERE org_id = $1 AND is_active = TRUE
    ORDER BY name ASC
) r;`

// AddMsgLabels inserts the passed in msg labels to our db
func AddMsgLabels(ctx context.Context, tx *sqlx.Tx, adds []*MsgLabelAdd) error {
	err := BulkQuery(ctx, "inserting msg labels", tx, sqlInsertMsgLabels, adds)
	return errors.Wrapf(err, "error inserting new msg labels")
}

const sqlInsertMsgLabels = `
INSERT INTO msgs_msg_labels(msg_id, label_id) VALUES(:msg_id, :label_id)
ON CONFLICT DO NOTHING`

// MsgLabelAdd represents a single label that should be added to a message
type MsgLabelAdd struct {
	MsgID   MsgID   `db:"msg_id"`
	LabelID LabelID `db:"label_id"`
}
