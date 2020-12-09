package internal

import (
	"net/http"

	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/core/models"
)

const (
	typeInternal = "internal"
)

func init() {
	models.RegisterTicketService(typeInternal, NewService)
}

type service struct {
	ticketer *flows.Ticketer
}

// NewService creates a new internal ticket service
func NewService(httpClient *http.Client, httpRetries *httpx.RetryConfig, ticketer *flows.Ticketer, config map[string]string) (models.TicketService, error) {
	return &service{ticketer: ticketer}, nil
}

// Open just returns a new ticket - no external service to notify
func (s *service) Open(session flows.Session, subject, body string, logHTTP flows.HTTPLogCallback) (*flows.Ticket, error) {
	return flows.NewTicket(flows.TicketUUID(uuids.New()), s.ticketer.Reference(), subject, body, ""), nil
}

// Forward is a noop
func (s *service) Forward(ticket *models.Ticket, msgUUID flows.MsgUUID, text string, attachments []utils.Attachment, logHTTP flows.HTTPLogCallback) error {
	return nil
}

// Close is a noop
func (s *service) Close(tickets []*models.Ticket, logHTTP flows.HTTPLogCallback) error {
	return nil
}

// Reopen is a noop
func (s *service) Reopen(tickets []*models.Ticket, logHTTP flows.HTTPLogCallback) error {
	return nil
}
