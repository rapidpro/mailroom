package mailgun

import (
	"fmt"
	"net/http"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/utils/httpx"
	"github.com/nyaruka/goflow/utils/uuids"
	"github.com/nyaruka/mailroom/models"

	"github.com/pkg/errors"
)

const (
	configDomain    = "domain"
	configAPIKey    = "api_key"
	configToAddress = "to_address"
)

func init() {
	models.RegisterTicketService("mailgun", NewService)
}

type service struct {
	client    *Client
	ticketer  *flows.Ticketer
	toAddress string
}

// NewService creates a new mailgun email-based ticket service
func NewService(httpClient *http.Client, httpRetries *httpx.RetryConfig, ticketer *flows.Ticketer, config map[string]string) (models.TicketService, error) {
	domain := config[configDomain]
	apiKey := config[configAPIKey]
	toAddress := config[configToAddress]
	if domain != "" && apiKey != "" && toAddress != "" {
		return &service{
			client:    NewClient(httpClient, httpRetries, domain, apiKey),
			ticketer:  ticketer,
			toAddress: toAddress,
		}, nil
	}
	return nil, errors.New("missing domain or api_key or to_address in mailgun config")
}

// Open opens a ticket which for mailgun means just sending an initial email
func (s *service) Open(session flows.Session, subject, body string, logHTTP flows.HTTPLogCallback) (*flows.Ticket, error) {
	ticketUUID := flows.TicketUUID(uuids.New())
	contactDisplay := session.Contact().Format(session.Environment())

	fromAddress, from := s.fromAddress(contactDisplay, ticketUUID)

	_, trace, err := s.client.SendMessage(from, s.toAddress, subject, body)
	if trace != nil {
		logHTTP(flows.NewHTTPLog(trace, flows.HTTPStatusFromCode))
	}
	if err != nil {
		return nil, errors.Wrap(err, "error calling mailgun API")
	}

	return flows.NewTicket(ticketUUID, s.ticketer.Reference(), subject, body, fromAddress), nil
}

func (s *service) Forward(ticket *models.Ticket, text string, logHTTP flows.HTTPLogCallback) error {
	ticketConfig := ticket.Config()
	contactDisplay, _ := ticketConfig.Map()["contact-display"].(string)

	_, from := s.fromAddress(contactDisplay, ticket.UUID())

	_, trace, err := s.client.SendMessage(from, s.toAddress, ticket.Subject(), text)
	if trace != nil {
		logHTTP(flows.NewHTTPLog(trace, flows.HTTPStatusFromCode))
	}
	if err != nil {
		return errors.Wrap(err, "error calling mailgun API")
	}

	return nil
}

func (s *service) fromAddress(contactDisplay string, ticketUUID flows.TicketUUID) (string, string) {
	if contactDisplay == "" {
		contactDisplay = "Contact"
	}
	address := fmt.Sprintf("ticket+%s@%s", ticketUUID, s.client.domain)
	withName := fmt.Sprintf("%s <%s>", contactDisplay, address)

	return address, withName
}
