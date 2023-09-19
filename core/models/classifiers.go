package models

import (
	"context"
	"database/sql"
	"database/sql/driver"

	"github.com/nyaruka/gocommon/dbutil"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/engine"
	"github.com/nyaruka/goflow/services/classification/bothub"
	"github.com/nyaruka/goflow/services/classification/luis"
	"github.com/nyaruka/goflow/services/classification/wit"
	"github.com/nyaruka/mailroom/core/goflow"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/null/v3"
	"github.com/pkg/errors"
)

// ClassifierID is our type for classifier IDs
type ClassifierID int

// NilClassifierID is nil value for classifier IDs
const NilClassifierID = ClassifierID(0)

// classifier type constants
const (
	ClassifierTypeWit    = "wit"
	ClassifierTypeLuis   = "luis"
	ClassifierTypeBothub = "bothub"
)

// classifier config key constants
const (
	// Wit.ai config options
	WitConfigAccessToken = "access_token"

	// Bothub.it config options
	BothubConfigAccessToken = "access_token"

	// LUIS config options
	LuisConfigAppID              = "app_id"
	LuisConfigPredictionEndpoint = "prediction_endpoint"
	LuisConfigPredictionKey      = "prediction_key"
	LuisConfigSlot               = "slot"
)

// Register a classification service factory with the engine
func init() {
	goflow.RegisterClassificationServiceFactory(classificationServiceFactory)
}

func classificationServiceFactory(c *runtime.Config) engine.ClassificationServiceFactory {
	return func(classifier *flows.Classifier) (flows.ClassificationService, error) {
		return classifier.Asset().(*Classifier).AsService(c, classifier)
	}
}

// Classifier is our type for a classifier
type Classifier struct {
	c struct {
		ID      ClassifierID          `json:"id"`
		UUID    assets.ClassifierUUID `json:"uuid"`
		Type    string                `json:"classifier_type"`
		Name    string                `json:"name"`
		Config  map[string]string     `json:"config"`
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

// AsService builds the corresponding ClassificationService for the passed in Classifier
func (c *Classifier) AsService(cfg *runtime.Config, classifier *flows.Classifier) (flows.ClassificationService, error) {
	httpClient, httpRetries, httpAccess := goflow.HTTP(cfg)

	switch c.Type() {
	case ClassifierTypeWit:
		accessToken := c.c.Config[WitConfigAccessToken]
		if accessToken == "" {
			return nil, errors.Errorf("missing %s for Wit classifier: %s", WitConfigAccessToken, c.UUID())
		}
		return wit.NewService(httpClient, httpRetries, classifier, accessToken), nil

	case ClassifierTypeLuis:
		appID := c.c.Config[LuisConfigAppID]
		endpoint := c.c.Config[LuisConfigPredictionEndpoint]
		key := c.c.Config[LuisConfigPredictionKey]
		slot := c.c.Config[LuisConfigSlot]
		if endpoint == "" || appID == "" || key == "" || slot == "" {
			return nil, errors.Errorf("missing %s, %s, %s or %s on LUIS classifier: %s",
				LuisConfigAppID, LuisConfigPredictionEndpoint, LuisConfigPredictionKey, LuisConfigSlot, c.UUID())
		}
		return luis.NewService(httpClient, httpRetries, httpAccess, classifier, endpoint, appID, key, slot), nil

	case ClassifierTypeBothub:
		accessToken := c.c.Config[BothubConfigAccessToken]
		if accessToken == "" {
			return nil, errors.Errorf("missing %s for Bothub classifier: %s", BothubConfigAccessToken, c.UUID())
		}
		return bothub.NewService(httpClient, httpRetries, classifier, accessToken), nil

	default:
		return nil, errors.Errorf("unknown classifier type '%s' for classifier: %s", c.Type(), c.UUID())
	}
}

// loadClassifiers loads all the classifiers for the passed in org
func loadClassifiers(ctx context.Context, db *sql.DB, orgID OrgID) ([]assets.Classifier, error) {
	rows, err := db.QueryContext(ctx, sqlSelectClassifiers, orgID)
	if err != nil {
		return nil, errors.Wrapf(err, "error querying classifiers for org: %d", orgID)
	}
	defer rows.Close()

	classifiers := make([]assets.Classifier, 0, 2)
	for rows.Next() {
		classifier := &Classifier{}
		err := dbutil.ScanJSON(rows, &classifier.c)
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

	return classifiers, nil
}

const sqlSelectClassifiers = `
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

func (i *ClassifierID) Scan(value any) error         { return null.ScanInt(value, i) }
func (i ClassifierID) Value() (driver.Value, error)  { return null.IntValue(i) }
func (i *ClassifierID) UnmarshalJSON(b []byte) error { return null.UnmarshalInt(b, i) }
func (i ClassifierID) MarshalJSON() ([]byte, error)  { return null.MarshalInt(i) }
