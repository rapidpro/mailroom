package models

import (
	"context"
	"database/sql"
	"database/sql/driver"

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
	ID_      ClassifierID          `json:"id"`
	UUID_    assets.ClassifierUUID `json:"uuid"`
	Type_    string                `json:"classifier_type"`
	Name_    string                `json:"name"`
	Config_  map[string]string     `json:"config"`
	Intents_ []struct {
		Name       string `json:"name"`
		ExternalID string `json:"external_id"`
	} `json:"intents"`

	intentNames []string
}

// ID returns the ID of this classifier
func (c *Classifier) ID() ClassifierID { return c.ID_ }

// UUID returns our UUID
func (c *Classifier) UUID() assets.ClassifierUUID { return c.UUID_ }

// Name return our Name
func (c *Classifier) Name() string { return c.Name_ }

// Intents returns a list of our intent names
func (c *Classifier) Intents() []string { return c.intentNames }

// Type returns the type of this classifier
func (c *Classifier) Type() string { return c.Type_ }

// AsService builds the corresponding ClassificationService for the passed in Classifier
func (c *Classifier) AsService(cfg *runtime.Config, classifier *flows.Classifier) (flows.ClassificationService, error) {
	httpClient, httpRetries, httpAccess := goflow.HTTP(cfg)

	switch c.Type() {
	case ClassifierTypeWit:
		accessToken := c.Config_[WitConfigAccessToken]
		if accessToken == "" {
			return nil, errors.Errorf("missing %s for Wit classifier: %s", WitConfigAccessToken, c.UUID())
		}
		return wit.NewService(httpClient, httpRetries, classifier, accessToken), nil

	case ClassifierTypeLuis:
		appID := c.Config_[LuisConfigAppID]
		endpoint := c.Config_[LuisConfigPredictionEndpoint]
		key := c.Config_[LuisConfigPredictionKey]
		slot := c.Config_[LuisConfigSlot]
		if endpoint == "" || appID == "" || key == "" || slot == "" {
			return nil, errors.Errorf("missing %s, %s, %s or %s on LUIS classifier: %s",
				LuisConfigAppID, LuisConfigPredictionEndpoint, LuisConfigPredictionKey, LuisConfigSlot, c.UUID())
		}
		return luis.NewService(httpClient, httpRetries, httpAccess, classifier, endpoint, appID, key, slot), nil

	case ClassifierTypeBothub:
		accessToken := c.Config_[BothubConfigAccessToken]
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

	clfs, err := ScanJSONRows(rows, func() assets.Classifier { return &Classifier{} })
	if err != nil {
		return nil, err
	}

	// populate our intent names
	for _, c := range clfs {
		classifier := c.(*Classifier)
		classifier.intentNames = make([]string, len(classifier.Intents_))
		for i, intent := range classifier.Intents_ {
			classifier.intentNames[i] = intent.Name
		}
	}

	return clfs, nil
}

const sqlSelectClassifiers = `
SELECT ROW_TO_JSON(r) FROM (
      SELECT c.id as id, c.uuid as uuid, c.name as name, c.classifier_type as classifier_type, c.config as config,
        (SELECT ARRAY_AGG(ci) FROM (
              SELECT ci.name as name, ci.external_id as external_id
                FROM classifiers_intent ci
               WHERE ci.classifier_id = c.id AND ci.is_active = TRUE
            ORDER BY ci.created_on ASC
        ) ci) as intents
        FROM classifiers_classifier c
       WHERE c.org_id = $1 AND c.is_active = TRUE
    ORDER BY c.created_on ASC
) r;`

func (i *ClassifierID) Scan(value any) error         { return null.ScanInt(value, i) }
func (i ClassifierID) Value() (driver.Value, error)  { return null.IntValue(i) }
func (i *ClassifierID) UnmarshalJSON(b []byte) error { return null.UnmarshalInt(b, i) }
func (i ClassifierID) MarshalJSON() ([]byte, error)  { return null.MarshalInt(i) }
