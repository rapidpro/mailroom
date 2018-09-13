package models

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/juju/errors"
	"github.com/nyaruka/goflow/utils"
)

type OrgID int

// Environment is mailroom's type for RapidPro orgs. It also implements the utils.Environment interface for GoFlow
type Org struct {
	id  OrgID
	env utils.Environment
}

// ID returns the id of the org
func (o *Org) ID() OrgID { return o.id }

// DateFormat returns the date format for this org
func (o *Org) DateFormat() utils.DateFormat { return o.env.DateFormat() }

// TimeFormat returns the time format for this org
func (o *Org) TimeFormat() utils.TimeFormat { return o.env.TimeFormat() }

// Timezone returns the timezone for this org
func (o *Org) Timezone() *time.Location { return o.env.Timezone() }

// Languages returns the list of supported languages for this org
func (o *Org) Languages() utils.LanguageList { return o.env.Languages() }

// RedactionPolicy returns the redaction policy (are we anonymous) for this org
func (o *Org) RedactionPolicy() utils.RedactionPolicy { return o.env.RedactionPolicy() }

// Now returns the current time in the current timezone for this org
func (o *Org) Now() time.Time { return o.env.Now() }

// Extension returns the extension for this org
func (o *Org) Extension(name string) json.RawMessage { return o.env.Extension(name) }

// loadOrg loads the org for the passed in id, returning any error encountered
func loadOrg(ctx context.Context, db sqlx.Queryer, orgID OrgID) (*Org, error) {
	org := &Org{}
	var orgJSON json.RawMessage
	rows, err := db.Query(selectOrgEnvironment, orgID)
	if err != nil {
		return nil, errors.Annotatef(err, "error loading org: %d", orgID)
	}
	defer rows.Close()

	rows.Next()
	err = rows.Scan(&org.id, &orgJSON)
	if err != nil {
		return nil, errors.Annotatef(err, "error scanning org: %d", orgID)
	}

	org.env, err = utils.ReadEnvironment(orgJSON)
	if err != nil {
		return nil, errors.Annotatef(err, "error unmarshalling org json: %s", orgJSON)
	}

	return org, nil
}

const selectOrgEnvironment = `
SELECT id, ROW_TO_JSON(o) FROM (SELECT
	id,
	(SELECT CASE date_format
		WHEN 'D' THEN 'DD-MM-YYYY'
		WHEN 'M' THEN 'MM-DD-YYYY'
	END) date_format,
	'tt:mm' as time_format,
	timezone,
	(SELECT CASE is_anon
		WHEN TRUE THEN 'urn'
		WHEN FALSE THEN 'none'
	END) redaction_policy,
	(SELECT 
		CASE
			WHEN language IS NOT NULL THEN ARRAY_PREPEND(language, ARRAY_AGG(l.iso_code)) 
			ELSE ARRAY_AGG(l.iso_code) 
		END 
		FROM (
			SELECT iso_code FROM orgs_language WHERE org_id = o.id
		) 
	l) languages
	FROM 
		orgs_org o
	WHERE
		o.id = $1
) o`
