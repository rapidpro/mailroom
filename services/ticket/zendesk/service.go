package zendesk

import (
	"net/http"
	"strconv"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/utils/httpx"
	"github.com/nyaruka/goflow/utils/uuids"
	"github.com/nyaruka/mailroom/models"

	"github.com/pkg/errors"
)

const (
	configSubdomain = "subdomain"
	configUsername  = "username"
	configAPIToken  = "api_token"
)

func init() {
	models.RegisterTicketService("zendesk", NewService)
}

type service struct {
	client   *Client
	ticketer *flows.Ticketer
}

// NewService creates a new zendesk ticket service
func NewService(httpClient *http.Client, httpRetries *httpx.RetryConfig, ticketer *flows.Ticketer, config map[string]string) (models.TicketService, error) {
	subdomain := config[configSubdomain]
	username := config[configUsername]
	apiToken := config[configAPIToken]
	if subdomain != "" && username != "" && apiToken != "" {
		return &service{
			client:   NewClient(httpClient, httpRetries, subdomain, username, apiToken),
			ticketer: ticketer,
		}, nil
	}
	return nil, errors.New("missing subdomain or username or api_token in zendesk config")
}

// Open opens a ticket which for mailgun means just sending an initial email
func (s *service) Open(session flows.Session, subject, body string, logHTTP flows.HTTPLogCallback) (*flows.Ticket, error) {
	ticketUUID := flows.TicketUUID(uuids.New())

	ticketResponse, trace, err := s.client.CreateTicket(subject, body)
	if trace != nil {
		logHTTP(flows.NewHTTPLog(trace, flows.HTTPStatusFromCode))
	}
	if err != nil {
		return nil, errors.Wrap(err, "error calling zendesk API")
	}

	return flows.NewTicket(ticketUUID, s.ticketer.Reference(), subject, body, strconv.Itoa(ticketResponse.ID)), nil
}

func (s *service) Forward(ticket *models.Ticket, text string, logHTTP flows.HTTPLogCallback) error {
	return nil
}
