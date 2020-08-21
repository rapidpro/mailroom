package models

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/services/airtime/dtone"
	"github.com/nyaruka/goflow/services/email/smtp"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/config"
	"github.com/nyaruka/mailroom/goflow"
	"github.com/nyaruka/mailroom/utils/storage"
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
	o struct {
		ID         OrgID    `json:"id"`
		Suspended  bool     `json:"is_suspended"`
		UsesTopups bool     `json:"uses_topups"`
		Config     null.Map `json:"config"`
	}
	env envs.Environment
}

// ID returns the id of the org
func (o *Org) ID() OrgID { return o.o.ID }

// Suspended returns whether the org has been suspended
func (o *Org) Suspended() bool { return o.o.Suspended }

// UsesTopups returns whether the org uses topups
func (o *Org) UsesTopups() bool { return o.o.UsesTopups }

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

// DefaultLocale combines the default languages and countries into a locale
func (o *Org) DefaultLocale() envs.Locale { return o.env.DefaultLocale() }

// LocationResolver returns a resolver for locations
func (o *Org) LocationResolver() envs.LocationResolver { return o.env.LocationResolver() }

// Equal return whether we are equal to the passed in environment
func (o *Org) Equal(env envs.Environment) bool { return o.env.Equal(env) }

// MarshalJSON is our custom marshaller so that our inner env get output
func (o *Org) MarshalJSON() ([]byte, error) {
	return json.Marshal(o.env)
}

// UnmarshalJSON is our custom unmarshaller
func (o *Org) UnmarshalJSON(b []byte) error {
	err := jsonx.Unmarshal(b, &o.o)
	if err != nil {
		return err
	}

	o.env, err = envs.ReadEnvironment(b)
	if err != nil {
		return err
	}
	return nil
}

// ConfigValue returns the string value for the passed in config (or default if not found)
func (o *Org) ConfigValue(key string, def string) string {
	return o.o.Config.GetString(key, def)
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

// StoreAttachment saves an attachment to storage
func (o *Org) StoreAttachment(s storage.Storage, prefix string, filename string, content []byte) (utils.Attachment, error) {
	contentType := http.DetectContentType(content)

	path := o.attachmentPath(prefix, filename)

	url, err := s.Put(path, contentType, content)
	if err != nil {
		return "", err
	}

	return utils.Attachment(contentType + ":" + url), nil
}

func (o *Org) attachmentPath(prefix string, filename string) string {
	parts := []string{prefix, fmt.Sprintf("%d", o.ID())}

	// not all filesystems like having a directory with a huge number of files, so if filename is long enough,
	// use parts of it to create intermediate subdirectories
	if len(filename) > 4 {
		parts = append(parts, filename[:4])

		if len(filename) > 8 {
			parts = append(parts, filename[4:8])
		}
	}
	parts = append(parts, filename)

	path := filepath.Join(parts...)

	// ensure path begins with /
	if !strings.HasPrefix(path, "/") {
		path = fmt.Sprintf("/%s", path)
	}

	return path
}

// gets the underlying org for the given engine session
func orgFromSession(session flows.Session) *Org {
	return session.Assets().Source().(*OrgAssets).Org()
}

// loadOrg loads the org for the passed in id, returning any error encountered
func loadOrg(ctx context.Context, db sqlx.Queryer, orgID OrgID) (*Org, error) {
	start := time.Now()

	org := &Org{}
	rows, err := db.Queryx(selectOrgByID, orgID, config.Mailroom.MaxValueLength)
	if err != nil {
		return nil, errors.Wrapf(err, "error loading org: %d", orgID)
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, errors.Errorf("no org with id: %d", orgID)
	}

	err = readJSONRow(rows, org)
	if err != nil {
		return nil, errors.Wrapf(err, "error unmarshalling org")
	}

	logrus.WithField("elapsed", time.Since(start)).WithField("org_id", orgID).Debug("loaded org environment")

	return org, nil
}

const selectOrgByID = `
SELECT ROW_TO_JSON(o) FROM (SELECT
	id,
	is_suspended,
	uses_topups,
	COALESCE(o.config::json,'{}'::json) AS config,
	(SELECT CASE date_format WHEN 'D' THEN 'DD-MM-YYYY' WHEN 'M' THEN 'MM-DD-YYYY' END) AS date_format, 
	'tt:mm' AS time_format,
	timezone,
	(SELECT CASE is_anon WHEN TRUE THEN 'urns' WHEN FALSE THEN 'none' END) AS redaction_policy,
	$2::int AS max_value_length,
	(SELECT iso_code FROM orgs_language WHERE id = o.primary_language_id) AS default_language,
	(SELECT ARRAY_AGG(iso_code) FROM orgs_language WHERE org_id = o.id) AS allowed_languages,
	COALESCE(
		(
			SELECT
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
	), '') AS default_country
	FROM 
		orgs_org o
	WHERE
		o.id = $1
) o`
