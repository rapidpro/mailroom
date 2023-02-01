package models

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/gocommon/dbutil"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/null/v2"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type TopicID int

const NilTopicID = TopicID(0)

type Topic struct {
	t struct {
		ID        TopicID          `json:"id"`
		UUID      assets.TopicUUID `json:"uuid"`
		OrgID     OrgID            `json:"org_id"`
		Name      string           `json:"name"`
		IsDefault bool             `json:"is_default"`
	}
}

// ID returns the ID
func (t *Topic) ID() TopicID { return t.t.ID }

// UUID returns the UUID
func (t *Topic) UUID() assets.TopicUUID { return t.t.UUID }

// OrgID returns the org ID
func (t *Topic) OrgID() OrgID { return t.t.OrgID }

// Name returns the name
func (t *Topic) Name() string { return t.t.Name }

// Type returns the type
func (t *Topic) IsDefault() bool { return t.t.IsDefault }

const selectOrgTopicsSQL = `
SELECT ROW_TO_JSON(r) FROM (SELECT
	t.id as id,
	t.uuid as uuid,
	t.org_id as org_id,
	t.name as name,
	t.is_default as is_default
FROM
	tickets_topic t
WHERE
	t.org_id = $1 AND
	t.is_active = TRUE
ORDER BY
	t.is_default DESC, t.created_on ASC
) r;
`

// loadTopics loads all the topics for the passed in org
func loadTopics(ctx context.Context, db sqlx.Queryer, orgID OrgID) ([]assets.Topic, error) {
	start := dates.Now()

	rows, err := db.Queryx(selectOrgTopicsSQL, orgID)
	if err != nil && err != sql.ErrNoRows {
		return nil, errors.Wrapf(err, "error querying topics for org: %d", orgID)
	}
	defer rows.Close()

	topics := make([]assets.Topic, 0, 2)
	for rows.Next() {
		topic := &Topic{}
		err := dbutil.ScanJSON(rows, &topic.t)
		if err != nil {
			return nil, errors.Wrapf(err, "error unmarshalling topic")
		}
		topics = append(topics, topic)
	}

	logrus.WithField("elapsed", time.Since(start)).WithField("org_id", orgID).WithField("count", len(topics)).Debug("loaded topics")

	return topics, nil
}

func (i *TopicID) Scan(value any) error         { return null.ScanInt(value, i) }
func (i TopicID) Value() (driver.Value, error)  { return null.IntValue(i) }
func (i *TopicID) UnmarshalJSON(b []byte) error { return null.UnmarshalInt(b, i) }
func (i TopicID) MarshalJSON() ([]byte, error)  { return null.MarshalInt(i) }
