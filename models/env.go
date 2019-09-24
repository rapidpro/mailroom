package models

import (
	"context"
	"encoding/json"
	"time"

	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/providers/airtime/dtone"
	"github.com/nyaruka/goflow/providers/webhooks"
	"github.com/nyaruka/mailroom/config"
	"github.com/nyaruka/mailroom/goflow"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	configDTOneLogin    = "TRANSFERTO_ACCOUNT_LOGIN"
	configDTOneToken    = "TRANSFERTO_AIRTIME_API_TOKEN"
	configDTOnecurrency = "TRANSFERTO_ACCOUNT_CURRENCY"
)

func init() {
	goflow.SetServiceResolver(&serviceResolver{})
}

type OrgID int

const NilOrgID = OrgID(0)

// Org is mailroom's type for RapidPro orgs. It also implements the envs.Environment interface for GoFlow
type Org struct {
	id     OrgID
	env    envs.Environment
	config map[string]interface{}
}

// ID returns the id of the org
func (o *Org) ID() OrgID { return o.id }

// DateFormat returns the date format for this org
func (o *Org) DateFormat() envs.DateFormat { return o.env.DateFormat() }

// NumberFormat returns the date format for this org
func (o *Org) NumberFormat() *envs.NumberFormat { return envs.DefaultNumberFormat }

// TimeFormat returns the time format for this org
func (o *Org) TimeFormat() envs.TimeFormat { return o.env.TimeFormat() }

// Timezone returns the timezone for this org
func (o *Org) Timezone() *time.Location { return o.env.Timezone() }

// DefaultLanguage returns the primary language for this org
func (o *Org) DefaultLanguage() envs.Language { return o.env.DefaultLanguage() }

// AllowedLanguages returns the list of supported languages for this org
func (o *Org) AllowedLanguages() []envs.Language { return o.env.AllowedLanguages() }

// RedactionPolicy returns the redaction policy (are we anonymous) for this org
func (o *Org) RedactionPolicy() envs.RedactionPolicy { return o.env.RedactionPolicy() }

// DefaultCountry returns the default country for this organization (mostly used for number parsing)
func (o *Org) DefaultCountry() envs.Country { return o.env.DefaultCountry() }

// Now returns the current time in the current timezone for this org
func (o *Org) Now() time.Time { return o.env.Now() }

// MaxValueLength returns our max value length for contact fields and run results
func (o *Org) MaxValueLength() int { return o.env.MaxValueLength() }

// Equal return whether we are equal to the passed in environment
func (o *Org) Equal(env envs.Environment) bool { return o.env.Equal(env) }

// MarshalJSON is our custom marshaller so that our inner env get output
func (o *Org) MarshalJSON() ([]byte, error) {
	return json.Marshal(o.env)
}

// ConfigValue returns the string value for the passed in config (or default if not found)
func (o *Org) ConfigValue(key string, def string) string {
	if o.config == nil {
		return def
	}

	val, found := o.config[key]
	if !found {
		return def
	}

	strVal, isStr := val.(string)
	if !isStr {
		return def
	}

	return strVal
}

// loadOrg loads the org for the passed in id, returning any error encountered
func loadOrg(ctx context.Context, db sqlx.Queryer, orgID OrgID) (*Org, error) {
	start := time.Now()

	org := &Org{}
	var orgJSON, orgConfig json.RawMessage
	rows, err := db.Query(selectOrgEnvironment, orgID, config.Mailroom.MaxValueLength)
	if err != nil {
		return nil, errors.Wrapf(err, "error loading org: %d", orgID)
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, errors.Errorf("no org with id: %d", orgID)
	}

	err = rows.Scan(&org.id, &orgConfig, &orgJSON)
	if err != nil {
		return nil, errors.Wrapf(err, "error scanning org: %d", orgID)
	}

	org.env, err = envs.ReadEnvironment(orgJSON)
	if err != nil {
		return nil, errors.Wrapf(err, "error unmarshalling org json: %s", orgJSON)
	}

	org.config = make(map[string]interface{})
	err = json.Unmarshal(orgConfig, &org.config)
	if err != nil {
		return nil, errors.Wrapf(err, "error unmarshalling org config: %s", orgConfig)
	}

	logrus.WithField("elapsed", time.Since(start)).WithField("org_id", orgID).Debug("loaded org environment")

	return org, nil
}

const selectOrgEnvironment = `
SELECT id, config, ROW_TO_JSON(o) FROM (SELECT
	id,
	COALESCE(o.config::json,'{}'::json) as config,
	(SELECT CASE date_format
		WHEN 'D' THEN 'DD-MM-YYYY'
		WHEN 'M' THEN 'MM-DD-YYYY'
	END) date_format,
	'tt:mm' as time_format,
	timezone,
	(SELECT CASE is_anon
		WHEN TRUE THEN 'urns'
		WHEN FALSE THEN 'none'
	END) redaction_policy,
	$2::int as max_value_length,
	(SELECT iso_code FROM orgs_language WHERE id = o.primary_language_id) as default_language,
	(SELECT ARRAY_AGG(iso_code) FROM orgs_language WHERE org_id = o.id) allowed_languages,
	COALESCE((SELECT
		country
	FROM
		channels_channel c
	WHERE
		c.org_id = o.id AND
		c.is_active = TRUE AND
		c.country IS NOT NULL
	GROUP BY
		c.country
	ORDER BY
		count(c.country) desc,
		country
	LIMIT 1
	), '') default_country
	FROM 
		orgs_org o
	WHERE
		o.id = $1
) o`

type serviceResolver struct{}

// Webhooks returns an a webhook provider for the given session
func (s *serviceResolver) Webhooks(flows.Session) flows.WebhookProvider {
	return webhooks.NewProvider("RapidProMailroom/"+config.Mailroom.Version, 10000)
}

// Airtime returns an airtime provider for the given session if the org has one configured
func (s *serviceResolver) Airtime(session flows.Session) flows.AirtimeProvider {
	org := session.Assets().Source().(*OrgAssets).Org()
	login := org.ConfigValue(configDTOneLogin, "")
	token := org.ConfigValue(configDTOneToken, "")
	currency := org.ConfigValue(configDTOnecurrency, "")

	if login != "" && token != "" {
		return dtone.NewProvider(login, token, currency)
	}
	return nil
}
