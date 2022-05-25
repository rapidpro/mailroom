package models

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/engine"
	"github.com/nyaruka/goflow/services/airtime/dtone"
	"github.com/nyaruka/goflow/services/email/smtp"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/goflow/utils/smtpx"
	"github.com/nyaruka/mailroom/core/goflow"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/utils/dbutil"
	"github.com/nyaruka/null"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// Register a airtime service factory with the engine
func init() {
	goflow.RegisterEmailServiceFactory(emailServiceFactory)
	goflow.RegisterAirtimeServiceFactory(airtimeServiceFactory)
}

func emailServiceFactory(c *runtime.Config) engine.EmailServiceFactory {
	var emailRetries = smtpx.NewFixedRetries(time.Second*3, time.Second*6)

	return func(session flows.Session) (flows.EmailService, error) {
		return orgFromSession(session).EmailService(c, emailRetries)
	}
}

func airtimeServiceFactory(c *runtime.Config) engine.AirtimeServiceFactory {
	// give airtime transfers an extra long timeout
	airtimeHTTPClient := &http.Client{Timeout: time.Duration(120 * time.Second)}
	airtimeHTTPRetries := httpx.NewFixedRetries(time.Second*5, time.Second*10)

	return func(session flows.Session) (flows.AirtimeService, error) {
		return orgFromSession(session).AirtimeService(airtimeHTTPClient, airtimeHTTPRetries)
	}
}

// OrgID is our type for orgs ids
type OrgID int

const (
	// NilOrgID is the id 0 considered as nil org id
	NilOrgID = OrgID(0)

	configSMTPServer  = "smtp_server"
	configDTOneKey    = "dtone_key"
	configDTOneSecret = "dtone_secret"
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
func (o *Org) EmailService(c *runtime.Config, retries *smtpx.RetryConfig) (flows.EmailService, error) {
	connectionURL := o.ConfigValue(configSMTPServer, c.SMTPServer)

	if connectionURL == "" {
		return nil, errors.New("missing SMTP configuration")
	}
	return smtp.NewService(connectionURL, retries)
}

// AirtimeService returns the airtime service for this org if one is configured
func (o *Org) AirtimeService(httpClient *http.Client, httpRetries *httpx.RetryConfig) (flows.AirtimeService, error) {
	key := o.ConfigValue(configDTOneKey, "")
	secret := o.ConfigValue(configDTOneSecret, "")

	if key == "" || secret == "" {
		return nil, errors.Errorf("missing %s or %s on DTOne configuration for org: %d", configDTOneKey, configDTOneSecret, o.ID())
	}
	return dtone.NewService(httpClient, httpRetries, key, secret), nil
}

// StoreAttachment saves an attachment to storage
func (o *Org) StoreAttachment(ctx context.Context, rt *runtime.Runtime, filename string, contentType string, content io.ReadCloser) (utils.Attachment, error) {
	prefix := rt.Config.S3MediaPrefix

	// read the content
	contentBytes, err := io.ReadAll(content)
	if err != nil {
		return "", errors.Wrapf(err, "unable to read attachment content")
	}
	content.Close()

	if contentType == "" {
		contentType = http.DetectContentType(contentBytes)
		contentType, _, _ = mime.ParseMediaType(contentType)
	}

	path := o.attachmentPath(prefix, filename)

	url, err := rt.MediaStorage.Put(ctx, path, contentType, contentBytes)
	if err != nil {
		return "", errors.Wrapf(err, "unable to store attachment content")
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

// LoadOrg loads the org for the passed in id, returning any error encountered
func LoadOrg(ctx context.Context, cfg *runtime.Config, db sqlx.Queryer, orgID OrgID) (*Org, error) {
	start := time.Now()

	org := &Org{}
	rows, err := db.Queryx(selectOrgByID, orgID, cfg.MaxValueLength)
	if err != nil {
		return nil, errors.Wrapf(err, "error loading org: %d", orgID)
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, errors.Errorf("no org with id: %d", orgID)
	}

	err = dbutil.ReadJSONRow(rows, org)
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
	flow_languages AS allowed_languages,
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
