package zendesk

import (
	"net/http"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/goflow/utils/dates"
	"github.com/nyaruka/goflow/utils/httpx"
	"github.com/nyaruka/goflow/utils/uuids"
	"github.com/nyaruka/mailroom/models"

	"github.com/pkg/errors"
)

const (
	configSubdomain      = "subdomain"
	configInstancePushID = "instance_push_id"
	configPushToken      = "push_token"
)

func init() {
	models.RegisterTicketService("zendesk", NewService)
}

type service struct {
	client         *Client
	ticketer       *flows.Ticketer
	redactor       utils.Redactor
	instancePushID string
}

// NewService creates a new zendesk ticket service
func NewService(httpClient *http.Client, httpRetries *httpx.RetryConfig, ticketer *flows.Ticketer, config map[string]string) (models.TicketService, error) {
	subdomain := config[configSubdomain]
	instancePushID := config[configInstancePushID]
	pushToken := config[configPushToken]
	if subdomain != "" && instancePushID != "" && pushToken != "" {
		return &service{
			client:         NewClient(httpClient, httpRetries, subdomain, pushToken),
			ticketer:       ticketer,
			redactor:       utils.NewRedactor(flows.RedactionMask, pushToken),
			instancePushID: instancePushID,
		}, nil
	}
	return nil, errors.New("missing subdomain or instance_push_id or push_token in zendesk config")
}

// Open opens a ticket which for mailgun means just sending an initial email
func (s *service) Open(session flows.Session, subject, body string, logHTTP flows.HTTPLogCallback) (*flows.Ticket, error) {
	ticketUUID := flows.TicketUUID(uuids.New())
	contactDisplay := session.Contact().Format(session.Environment())

	msg := &ExternalResource{
		ExternalID: string(ticketUUID),
		Message:    body,
		ThreadID:   string(ticketUUID),
		CreatedAt:  dates.Now(),
		Author: Author{
			ExternalID: string(session.Contact().UUID()),
			Name:       contactDisplay,
		},
		AllowChannelback: true,
	}

	if err := s.push(msg, logHTTP); err != nil {
		return nil, err
	}

	return flows.NewTicket(ticketUUID, s.ticketer.Reference(), subject, body, ""), nil
}

func (s *service) Forward(ticket *models.Ticket, contact *models.Contact, msgUUID flows.MsgUUID, text string, logHTTP flows.HTTPLogCallback) error {
	ticketConfig := ticket.Config()
	contactDisplay, _ := ticketConfig.Map()["contact-display"].(string)

	msg := &ExternalResource{
		ExternalID: string(msgUUID),
		Message:    text,
		ThreadID:   string(ticket.UUID()),
		CreatedAt:  dates.Now(),
		Author: Author{
			ExternalID: string(contact.UUID()),
			Name:       contactDisplay,
		},
		AllowChannelback: true,
	}

	return s.push(msg, logHTTP)
}

func (s *service) push(msg *ExternalResource, logHTTP flows.HTTPLogCallback) error {
	results, trace, err := s.client.Push(s.instancePushID, []*ExternalResource{msg})
	if trace != nil {
		logHTTP(flows.NewHTTPLog(trace, flows.HTTPStatusFromCode, s.redactor))
	}
	if err != nil || results[0].Status.Code != "success" {
		if err == nil {
			err = errors.New(results[0].Status.Description)
		}
		return errors.Wrap(err, "error pushing message to zendesk")
	}
	return nil
}
