package mailgun

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"
	"text/template"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/goflow/utils/httpx"
	"github.com/nyaruka/goflow/utils/uuids"
	"github.com/nyaruka/mailroom/models"

	"github.com/pkg/errors"
)

const (
	typeMailgun = "mailgun"

	configDomain    = "domain"
	configAPIKey    = "api_key"
	configToAddress = "to_address"
	configURLBase   = "url_base"
)

// body text template for messages being forwarded from contact
var forwardTextTemplate = template.Must(template.New("forward_text").Parse(`{{.message}}

------------------------------------------------
You can close this ticket by replying with CLOSE
You can view this contact at {{.contact_url}}
`))

func init() {
	models.RegisterTicketService(typeMailgun, NewService)
}

type service struct {
	client    *Client
	ticketer  *flows.Ticketer
	toAddress string
	urlBase   string
	redactor  utils.Redactor
}

// NewService creates a new mailgun email-based ticket service
func NewService(httpClient *http.Client, httpRetries *httpx.RetryConfig, ticketer *flows.Ticketer, config map[string]string) (models.TicketService, error) {
	domain := config[configDomain]
	apiKey := config[configAPIKey]
	toAddress := config[configToAddress]
	urlBase := config[configURLBase]

	if domain != "" && apiKey != "" && toAddress != "" && urlBase != "" {
		// need to redact the string used for basic auth
		basicAuth := base64.StdEncoding.EncodeToString([]byte("api:" + apiKey))

		return &service{
			client:    NewClient(httpClient, httpRetries, domain, apiKey),
			ticketer:  ticketer,
			toAddress: toAddress,
			urlBase:   urlBase,
			redactor:  utils.NewRedactor(flows.RedactionMask, apiKey, basicAuth),
		}, nil
	}
	return nil, errors.New("missing domain or api_key or to_address or url_base in mailgun config")
}

// Open opens a ticket which for mailgun means just sending an initial email
func (s *service) Open(session flows.Session, subject, body string, logHTTP flows.HTTPLogCallback) (*flows.Ticket, error) {
	ticketUUID := flows.TicketUUID(uuids.New())
	contactDisplay := session.Contact().Format(session.Environment())

	from := s.fromAddress(contactDisplay, ticketUUID)
	emailBody := s.createBody(forwardTextTemplate, body, string(session.Contact().UUID()))

	msgID, trace, err := s.client.SendMessage(from, s.toAddress, subject, emailBody, nil)
	if trace != nil {
		logHTTP(flows.NewHTTPLog(trace, flows.HTTPStatusFromCode, s.redactor))
	}
	if err != nil {
		return nil, errors.Wrap(err, "error calling mailgun API")
	}

	return flows.NewTicket(ticketUUID, s.ticketer.Reference(), subject, body, msgID), nil
}

func (s *service) Forward(ticket *models.Ticket, msgUUID flows.MsgUUID, text string, logHTTP flows.HTTPLogCallback) error {
	contactUUID := ticket.Config("contact-uuid")
	body := s.createBody(forwardTextTemplate, text, contactUUID)

	lastSubject := ticket.Config("last-subject")
	if lastSubject == "" {
		lastSubject = ticket.Subject()
	}

	return s.send(ticket, lastSubject, body, logHTTP)
}

func (s *service) Close(tickets []*models.Ticket, logHTTP flows.HTTPLogCallback) error {
	// TODO send emails to tell ticket handlers that they've been closed
	return nil
}

func (s *service) Reopen(tickets []*models.Ticket, logHTTP flows.HTTPLogCallback) error {
	// TODO send emails to tell ticket handlers that they've been reopend
	return nil
}

func (s *service) send(ticket *models.Ticket, subject, text string, logHTTP flows.HTTPLogCallback) error {
	contactDisplay := ticket.Config("contact-display")
	lastMessageID := ticket.Config("last-message-id")
	from := s.fromAddress(contactDisplay, ticket.UUID())

	_, trace, err := s.client.SendMessage(from, s.toAddress, subject, text, map[string]string{"In-Reply-To": lastMessageID})
	if trace != nil {
		logHTTP(flows.NewHTTPLog(trace, flows.HTTPStatusFromCode, s.redactor))
	}
	if err != nil {
		return errors.Wrap(err, "error calling mailgun API")
	}

	return nil
}

func (s *service) fromAddress(contactDisplay string, ticketUUID flows.TicketUUID) string {
	address := fmt.Sprintf("ticket+%s@%s", ticketUUID, s.client.domain)

	if contactDisplay == "" {
		return address
	}

	return fmt.Sprintf("%s <%s>", contactDisplay, address)
}

func (s *service) createBody(tpl *template.Template, message, contactUUID string) string {
	context := map[string]string{
		"message":     message,
		"contact_url": fmt.Sprintf("%s/contact/read/%s/", s.urlBase, contactUUID),
	}

	b := &strings.Builder{}
	tpl.Execute(b, context)
	return b.String()
}
