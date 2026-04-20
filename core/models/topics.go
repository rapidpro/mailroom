package models

import (
	"context"
	"database/sql"
	"database/sql/driver"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/null/v3"
	"github.com/pkg/errors"
)

type TopicID int

const NilTopicID = TopicID(0)

type Topic struct {
	ID_        TopicID          `json:"id"`
	UUID_      assets.TopicUUID `json:"uuid"`
	OrgID_     OrgID            `json:"org_id"`
	Name_      string           `json:"name"`
	IsDefault_ bool             `json:"is_default"`
}

// ID returns the ID
func (t *Topic) ID() TopicID { return t.ID_ }

// UUID returns the UUID
func (t *Topic) UUID() assets.TopicUUID { return t.UUID_ }

// Name returns the name
func (t *Topic) Name() string { return t.Name_ }

// Type returns the type
func (t *Topic) IsDefault() bool { return t.IsDefault_ }

const sqlSelectTopicsByOrg = `
SELECT ROW_TO_JSON(r) FROM (
      SELECT t.id as id, t.uuid as uuid, t.org_id as org_id, t.name as name, t.is_default as is_default
        FROM tickets_topic t
       WHERE t.org_id = $1 AND t.is_active = TRUE
    ORDER BY t.is_default DESC, t.created_on ASC
) r;`

// loadTopics loads all the topics for the passed in org
func loadTopics(ctx context.Context, db *sql.DB, orgID OrgID) ([]assets.Topic, error) {
	rows, err := db.QueryContext(ctx, sqlSelectTopicsByOrg, orgID)
	if err != nil && err != sql.ErrNoRows {
		return nil, errors.Wrapf(err, "error querying topics for org: %d", orgID)
	}

	return ScanJSONRows(rows, func() assets.Topic { return &Topic{} })
}

func (i *TopicID) Scan(value any) error         { return null.ScanInt(value, i) }
func (i TopicID) Value() (driver.Value, error)  { return null.IntValue(i) }
func (i *TopicID) UnmarshalJSON(b []byte) error { return null.UnmarshalInt(b, i) }
func (i TopicID) MarshalJSON() ([]byte, error)  { return null.MarshalInt(i) }
