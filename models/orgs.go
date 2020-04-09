package models

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/services/airtime/dtone"
	"github.com/nyaruka/goflow/services/email/smtp"
	"github.com/nyaruka/goflow/utils/httpx"
	"github.com/nyaruka/mailroom/config"
	"github.com/nyaruka/mailroom/goflow"
	"github.com/nyaruka/null"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// Register a airtime service factory with the engine
func init() {
	// give airtime transfers an extra long timeout
	airtimeHTTPClient := &http.Client{Timeout: time.Duration(120 * time.Second)}
	airtimeHTTPRetries := httpx.NewFixedRetries(5, 10)

	goflow.RegisterEmailServiceFactory(
		func(session flows.Session) (flows.EmailService, error) {
			return orgFromSession(session).EmailService(http.DefaultClient)
		},
	)

	goflow.RegisterAirtimeServiceFactory(
		func(session flows.Session) (flows.AirtimeService, error) {
			return orgFromSession(session).AirtimeService(airtimeHTTPClient, airtimeHTTPRetries)
		},
	)
}

// OrgID is our type for orgs ids
type OrgID int

// UserID is our type for user ids used by modified_by, which can be null
type UserID null.Int

const (
	// NilOrgID is the id 0 considered as nil org id
	NilOrgID = OrgID(0)

	// NilUserID si the id 0 considered as nil user id
	NilUserID = UserID(0)

	configSMTPServer    = "smtp_server"
	configDTOneLogin    = "TRANSFERTO_ACCOUNT_LOGIN"
	configDTOneToken    = "TRANSFERTO_AIRTIME_API_TOKEN"
	configDTOnecurrency = "TRANSFERTO_ACCOUNT_CURRENCY"
)

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

// EmailService returns the email service for this org
func (o *Org) EmailService(httpClient *http.Client) (flows.EmailService, error) {
	connectionURL := o.ConfigValue(configSMTPServer, config.Mailroom.SMTPServer)

	if connectionURL == "" {
		return nil, errors.New("missing SMTP configuration")
	}
	return smtp.NewServiceFromURL(connectionURL)
}

// AirtimeService returns the airtime service for this org if one is configured
func (o *Org) AirtimeService(httpClient *http.Client, httpRetries *httpx.RetryConfig) (flows.AirtimeService, error) {
	login := o.ConfigValue(configDTOneLogin, "")
	token := o.ConfigValue(configDTOneToken, "")
	currency := o.ConfigValue(configDTOnecurrency, "")

	if login == "" || token == "" {
		return nil, errors.Errorf("missing %s or %s on DTOne configuration for org: %d", configDTOneLogin, configDTOneToken, o.ID())
	}
	return dtone.NewService(httpClient, httpRetries, login, token, currency), nil
}

// gets the underlying org for the given engine session
func orgFromSession(session flows.Session) *Org {
	return session.Assets().Source().(*OrgAssets).Org()
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
