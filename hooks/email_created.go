package hooks

import (
	"context"
	"net/url"
	"strconv"

	"github.com/go-mail/mail"
	"github.com/nyaruka/mailroom/config"
	"github.com/pkg/errors"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/models"
	"github.com/sirupsen/logrus"
)

func init() {
	models.RegisterEventHook(events.TypeEmailCreated, handleEmailCreated)
}

// SendEmailsHook is our hook for sending emails
type SendEmailsHook struct{}

var sendEmailsHook = &SendEmailsHook{}

const (
	configSMTPServer = "SMTP_SERVER"
)

// Apply sends all our emails
func (h *SendEmailsHook) Apply(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, sessions map[*models.Session][]interface{}) error {
	// get our smtp server config
	config := org.Org().ConfigValue(configSMTPServer, config.Mailroom.SMTPServer)

	// no config? noop
	if config == "" {
		logrus.WithField("org_id", org.OrgID()).Warn("no smtp settings set, ignoring email event")
		return nil
	}

	// parse it
	url, err := url.Parse(config)
	if err != nil {
		return errors.Wrapf(err, "unable to parse smtp config: %s", config)
	}

	// figure out our port
	sPort := url.Port()
	if sPort == "" {
		sPort = "25"
	}
	port, err := strconv.Atoi(sPort)
	if err != nil {
		return errors.Wrapf(err, "invalid port configuration: %s", config)
	}

	// and our user and password
	if url.User == nil {
		return errors.Wrapf(err, "no user set for smtp server: %s", config)
	}
	password, _ := url.User.Password()

	// get our from
	from := url.Query()["from"]
	if len(from) == 0 {
		from = []string{url.User.Username()}
	}

	// create our dialer for our org
	d := mail.NewDialer(url.Hostname(), port, url.User.Username(), password)

	// send each of our emails, errors are logged but don't stop us from trying to send our other emails
	for _, es := range sessions {
		for _, e := range es {
			email := e.(*events.EmailCreatedEvent)

			m := mail.NewMessage()
			m.SetHeader("From", from[0])
			m.SetHeader("To", email.Addresses...)
			m.SetHeader("Subject", email.Subject)
			m.SetBody("text/plain", email.Body)

			err = d.DialAndSend(m)
			if err != nil {
				// TODO: how can we expose these errors to end users somehow?
				logrus.WithFields(logrus.Fields{
					"smtp_server": config,
					"subject":     email.Subject,
					"org_id":      org.OrgID(),
				}).WithError(err).Error("error sending email")
			}
		}
	}

	return nil
}

// handleEmailCreated event queues an email to be sent later on
func handleEmailCreated(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, session *models.Session, e flows.Event) error {
	event := e.(*events.EmailCreatedEvent)
	logrus.WithFields(logrus.Fields{
		"contact_uuid": session.ContactUUID(),
		"session_id":   session.ID,
		"body":         event.Body,
		"addresses":    event.Addresses,
	}).Debug("creating email")

	// register to send this email after our session is committed
	session.AddPostCommitEvent(sendEmailsHook, event)

	return nil
}
