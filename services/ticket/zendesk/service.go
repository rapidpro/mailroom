package zendesk

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
	configSubdomain  = "subdomain"
	configOAuthToken = "oauth_token"
)

func init() {
	models.RegisterTicketService("zendesk", NewService)
}

type service struct {
	client   *Client
	ticketer *flows.Ticketer
	redactor utils.Redactor
}

// NewService creates a new zendesk ticket service
func NewService(httpClient *http.Client, httpRetries *httpx.RetryConfig, ticketer *flows.Ticketer, config map[string]string) (models.TicketService, error) {
	subdomain := config[configSubdomain]
	oAuthToken := config[configOAuthToken]
	if subdomain != "" && oAuthToken != "" {
		return &service{
			client:   NewClient(httpClient, httpRetries, subdomain, oAuthToken),
			ticketer: ticketer,
			redactor: utils.NewRedactor(flows.RedactionMask, oAuthToken),
		}, nil
	}
	return nil, errors.New("missing subdomain or oauth_token in zendesk config")
}

// Open opens a ticket which for mailgun means just sending an initial email
func (s *service) Open(session flows.Session, subject, body string, logHTTP flows.HTTPLogCallback) (*flows.Ticket, error) {
	ticketUUID := flows.TicketUUID(uuids.New())

	name := session.Contact().Format(session.Environment())

	zenUser, trace, err := s.client.CreateOrUpdateUser(name, "end-user", string(session.Contact().UUID()))
	if trace != nil {
		logHTTP(flows.NewHTTPLog(trace, flows.HTTPStatusFromCode, s.redactor))
	}
	if err != nil {
		return nil, errors.Wrap(err, "error creating zendesk user")
	}

	zenTicket, trace, err := s.client.CreateTicket(zenUser.ID, subject, body)
	if trace != nil {
		logHTTP(flows.NewHTTPLog(trace, flows.HTTPStatusFromCode, s.redactor))
	}
	if err != nil {
		return nil, errors.Wrap(err, "error creating zendesk ticket")
	}

	return flows.NewTicket(ticketUUID, s.ticketer.Reference(), subject, body, fmt.Sprintf("%d", zenTicket.ID)), nil
}

func (s *service) Forward(ticket *models.Ticket, text string, logHTTP flows.HTTPLogCallback) error {
	return nil
}
