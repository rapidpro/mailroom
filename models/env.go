package models

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/utils"
	"github.com/pkg/errors"
)

type OrgID int

// Org is mailroom's type for RapidPro orgs. It also implements the utils.Environment interface for GoFlow
type Org struct {
	id     OrgID
	env    utils.Environment
	config map[string]interface{}
}

// ID returns the id of the org
func (o *Org) ID() OrgID { return o.id }

// DateFormat returns the date format for this org
func (o *Org) DateFormat() utils.DateFormat { return o.env.DateFormat() }

// NumberFormat returns the date format for this org
func (o *Org) NumberFormat() *utils.NumberFormat { return utils.DefaultNumberFormat }

// TimeFormat returns the time format for this org
func (o *Org) TimeFormat() utils.TimeFormat { return o.env.TimeFormat() }

// Timezone returns the timezone for this org
func (o *Org) Timezone() *time.Location { return o.env.Timezone() }

// DefaultLanguage returns the primary language for this org
func (o *Org) DefaultLanguage() utils.Language { return o.env.DefaultLanguage() }

// AllowedLanguages returns the list of supported languages for this org
func (o *Org) AllowedLanguages() []utils.Language { return o.env.AllowedLanguages() }

// RedactionPolicy returns the redaction policy (are we anonymous) for this org
func (o *Org) RedactionPolicy() utils.RedactionPolicy { return o.env.RedactionPolicy() }

// Now returns the current time in the current timezone for this org
func (o *Org) Now() time.Time { return o.env.Now() }

// Extension returns the extension for this org
func (o *Org) Extension(name string) json.RawMessage { return o.env.Extension(name) }

// Equal return whether we are equal to the passed in environment
func (o *Org) Equal(env utils.Environment) bool { return o.env.Equal(env) }

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
	org := &Org{}
	var orgJSON, orgConfig json.RawMessage
	rows, err := db.Query(selectOrgEnvironment, orgID)
	if err != nil {
		return nil, errors.Wrapf(err, "error loading org: %d", orgID)
	}
	defer rows.Close()

	rows.Next()
	err = rows.Scan(&org.id, &orgConfig, &orgJSON)
	if err != nil {
		return nil, errors.Wrapf(err, "error scanning org: %d", orgID)
	}

	org.env, err = utils.ReadEnvironment(orgJSON)
	if err != nil {
		return nil, errors.Wrapf(err, "error unmarshalling org json: %s", orgJSON)
	}

	org.config = make(map[string]interface{})
	err = json.Unmarshal(orgConfig, &org.config)
	if err != nil {
		return nil, errors.Wrapf(err, "error unmarshalling org config: %s", orgConfig)
	}

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
	COALESCE(language, '') as default_language,
	(SELECT 
		CASE
			WHEN language IS NOT NULL THEN ARRAY_PREPEND(language, ARRAY_AGG(l.iso_code)) 
			ELSE ARRAY_AGG(l.iso_code) 
		END 
		FROM (
			SELECT iso_code FROM orgs_language WHERE org_id = o.id
		) 
	l) allowed_languages
	FROM 
		orgs_org o
	WHERE
		o.id = $1
) o`
