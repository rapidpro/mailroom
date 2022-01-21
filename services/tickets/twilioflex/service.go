package twilioflex

import (
	"net/http"

	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
)

const (
	typeTwilioFlex              = "twilioflex"
	configurationAuthToken      = "auth_token"
	configurationAccountSID     = "account_sid"
	configurationChatServiceSID = "chat_service_sid"
	configurationWorkspaceSID   = "workspace_sid"
)

func init() {
	models.RegisterTicketService(typeTwilioFlex, NewService)
}

type service struct {
	ticketer *flows.Ticketer
}

func NewService(rtCfg *runtime.Config, httpClient *http.Client, httpRetries *httpx.RetryConfig, ticketer *flows.Ticketer, config map[string]string) (models.TicketService, error) {
	return &service{
		ticketer: ticketer,
	}, nil
}

func (s *service) Open(session flows.Session, topic *flows.Topic, body string, assignee *flows.User, logHTTP flows.HTTPLogCallback) (*flows.Ticket, error) {
	// TODO: Open Ticket
	return nil, nil
}

func (s *service) Forward(ticket *models.Ticket, msgUUID flows.MsgUUID, text string, attachments []utils.Attachment, logHTTP flows.HTTPLogCallback) error {
	// TODO: Forward
	return nil
}

func (s *service) Close(tickets []*models.Ticket, logHTTP flows.HTTPLogCallback) error {
	// TODO: Close
	return nil
}

func (s *service) Reopen(tickets []*models.Ticket, logHTTP flows.HTTPLogCallback) error {
	// TODO: Reopen
	return nil
}
