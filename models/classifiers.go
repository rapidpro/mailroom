package models

import (
	"context"
	"database/sql/driver"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/null"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// ClassifierID is our type for classifier ids
type ClassifierID null.Int

// NilClassifierID is our const for a nil classifier ID
const NilClassifierID = ClassifierID(0)

// Classifier is our type for a Classifier
type Classifier struct {
	c struct {
		ID      ClassifierID           `json:"id"`
		UUID    assets.ClassifierUUID  `json:"uuid"`
		Type    string                 `json:"classifier_type"`
		Name    string                 `json:"name"`
		Config  map[string]interface{} `json:"config"`
		Intents []struct {
			Name       string `json:"name"`
			ExternalID string `json:"external_id"`
		} `json:"intents"`

		intentNames []string
	}
}

// ID returns the ID of this classifier
func (c *Classifier) ID() ClassifierID { return c.c.ID }

// UUID returns our UUID
func (c *Classifier) UUID() assets.ClassifierUUID { return c.c.UUID }

// Name return our Name
func (c *Classifier) Name() string { return c.c.Name }

// Intents returns a list of our intent names
func (c *Classifier) Intents() []string { return c.c.intentNames }

// Type returns the type of this classifier
func (c *Classifier) Type() string { return c.c.Type }

// loadClassifiers loads all the classifiers for the passed in org
func loadClassifiers(ctx context.Context, db sqlx.Queryer, orgID OrgID) ([]assets.Classifier, error) {
	start := time.Now()

	rows, err := db.Queryx(selectClassifiersSQL, orgID)
	if err != nil {
		return nil, errors.Wrapf(err, "error querying classifiers for org: %d", orgID)
	}
	defer rows.Close()

	classifiers := make([]assets.Classifier, 0, 2)
	for rows.Next() {
		classifier := &Classifier{}
		err := readJSONRow(rows, &classifier.c)
		if err != nil {
			return nil, errors.Wrapf(err, "error unmarshalling classifier")
		}

		// populate our intent names
		classifier.c.intentNames = make([]string, len(classifier.c.Intents))
		for i, intent := range classifier.c.Intents {
			classifier.c.intentNames[i] = intent.Name
		}

		classifiers = append(classifiers, classifier)
	}

	logrus.WithField("elapsed", time.Since(start)).WithField("org_id", orgID).WithField("count", len(classifiers)).Debug("loaded classifiers")

	return classifiers, nil
}

const selectClassifiersSQL = `
SELECT ROW_TO_JSON(r) FROM (SELECT
	c.id as id,
	c.uuid as uuid,
	c.name as name,
	c.classifier_type as classifier_type,
	c.config as config,
	(SELECT ARRAY_AGG(ci) FROM (
		SELECT
			ci.name as name,
			ci.external_id as external_id
		FROM
			classifiers_intent ci
		WHERE
			ci.classifier_id = c.id AND
			ci.is_active = TRUE
		ORDER BY
			ci.created_on ASC
	) ci) as intents
FROM 
	classifiers_classifier c
WHERE 
	c.org_id = $1 AND 
	c.is_active = TRUE
ORDER BY
	c.created_on ASC
) r;
`

// MarshalJSON marshals into JSON. 0 values will become null
func (i ClassifierID) MarshalJSON() ([]byte, error) {
	return null.Int(i).MarshalJSON()
}

// UnmarshalJSON unmarshals from JSON. null values become 0
func (i *ClassifierID) UnmarshalJSON(b []byte) error {
	return null.UnmarshalInt(b, (*null.Int)(i))
}

// Value returns the db value, null is returned for 0
func (i ClassifierID) Value() (driver.Value, error) {
	return null.Int(i).Value()
}

// Scan scans from the db value. null values become 0
func (i *ClassifierID) Scan(value interface{}) error {
	return null.ScanInt(value, (*null.Int)(i))
}
