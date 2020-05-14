package mailgun

import (
	"fmt"
	"net/http"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/goflow/utils/httpx"
	"github.com/nyaruka/goflow/utils/uuids"
	"github.com/nyaruka/mailroom/models"

	"github.com/pkg/errors"
)

const (
	configDomain    = "domain"
	configAPIKey    = "api_key"
	configToAddress = "to_address"
	configURLBase   = "url_base"

	bodyTextTemplate = `%s

------------------------------------------------
You can close this ticket by replying with CLOSE
You can view this contact at %s
`
)

func init() {
	models.RegisterTicketService("mailgun", NewService)
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
		return &service{
			client:    NewClient(httpClient, httpRetries, domain, apiKey),
			ticketer:  ticketer,
			toAddress: toAddress,
			urlBase:   urlBase,
			redactor:  utils.NewRedactor(flows.RedactionMask, apiKey),
		}, nil
	}
	return nil, errors.New("missing domain or api_key or to_address or url_base in mailgun config")
}

// Open opens a ticket which for mailgun means just sending an initial email
func (s *service) Open(session flows.Session, subject, body string, logHTTP flows.HTTPLogCallback) (*flows.Ticket, error) {
	ticketUUID := flows.TicketUUID(uuids.New())
	contactDisplay := session.Contact().Format(session.Environment())

	from := s.fromAddress(contactDisplay, ticketUUID)
	emailBody := s.createBody(body, session.Contact().UUID())

	msgID, trace, err := s.client.SendMessage(from, s.toAddress, subject, emailBody, "")
	if trace != nil {
		logHTTP(flows.NewHTTPLog(trace, flows.HTTPStatusFromCode, s.redactor))
	}
	if err != nil {
		return nil, errors.Wrap(err, "error calling mailgun API")
	}

	return flows.NewTicket(ticketUUID, s.ticketer.Reference(), subject, body, msgID), nil
}

func (s *service) Forward(ticket *models.Ticket, contact *models.Contact, msgUUID flows.MsgUUID, text string, logHTTP flows.HTTPLogCallback) error {
	ticketConfig := ticket.Config()
	contactDisplay, _ := ticketConfig.Map()["contact-display"].(string)
	lastMessageID, _ := ticketConfig.Map()["last-message-id"].(string)
	lastSubject, _ := ticketConfig.Map()["last-subject"].(string)

	if lastSubject == "" {
		lastSubject = ticket.Subject()
	}

	from := s.fromAddress(contactDisplay, ticket.UUID())
	body := s.createBody(text, contact.UUID())

	_, trace, err := s.client.SendMessage(from, s.toAddress, lastSubject, body, lastMessageID)
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

func (s *service) createBody(base string, contactUUID flows.ContactUUID) string {
	contactURL := fmt.Sprintf("%s/contact/read/%s/", s.urlBase, contactUUID)

	return fmt.Sprintf(bodyTextTemplate, base, contactURL)
}
