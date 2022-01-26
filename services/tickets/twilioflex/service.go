package twilioflex

import (
	"net/http"

	"github.com/pkg/errors"

	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
)

const (
	typeTwilioFlex              = "twilioflex"
	configurationAuthToken      = "auth_token"
	configurationAccountSid     = "account_sid"
	configurationChatServiceSid = "chat_service_sid"
	configurationWorkspaceSid   = "workspace_sid"
	configurationWorkflowSid    = "workflow_sid"
	configurationTaskChannelSid = "task_channel_sid"
	configurationFlexFlowSid    = "flex_flow_sid"
)

func init() {
	models.RegisterTicketService(typeTwilioFlex, NewService)
}

type service struct {
	rtConfig   *runtime.Config
	restClient *RESTClient
	ticketer   *flows.Ticketer
	redactor   utils.Redactor
}

// newService creates a new twilio flex ticket service
func NewService(rtCfg *runtime.Config, httpClient *http.Client, httpRetries *httpx.RetryConfig, ticketer *flows.Ticketer, config map[string]string) (models.TicketService, error) {
	authToken := config[configurationAuthToken]
	accountSid := config[configurationAccountSid]
	chatServiceSid := config[configurationChatServiceSid]
	workspaceSid := config[configurationWorkspaceSid]
	workflowSid := config[configurationWorkflowSid]
	taskChannelSid := config[configurationTaskChannelSid]
	flexFlowSid := config[configurationFlexFlowSid]
	if authToken != "" && accountSid != "" && chatServiceSid != "" && workspaceSid != "" && workflowSid != "" && taskChannelSid != "" {
		return &service{
			rtConfig:   rtCfg,
			ticketer:   ticketer,
			restClient: NewRestClient(httpClient, httpRetries, authToken, accountSid, chatServiceSid, workspaceSid, workflowSid, taskChannelSid, flexFlowSid),
			redactor:   utils.NewRedactor(flows.RedactionMask, authToken, accountSid, chatServiceSid, workspaceSid),
		}, nil
	}
	return nil, errors.New("missing auth_token or account_sid or chat_service_sid or workspace_sid in twilio flex config")
}

func (s *service) Open(session flows.Session, topic *flows.Topic, body string, assignee *flows.User, logHTTP flows.HTTPLogCallback) (*flows.Ticket, error) {
	ticket := flows.OpenTicket(s.ticketer, topic, body, assignee)
	contact := session.Contact()
	chatUser := &CreateChatUserParams{
		Identity:     string(contact.UUID()),
		FriendlyName: contact.Name(),
	}
	contactUser, trace, err := s.restClient.GetUser(chatUser.Identity)
	if trace != nil {
		logHTTP(flows.NewHTTPLog(trace, flows.HTTPStatusFromCode, s.redactor))
	}
	if err != nil && trace.Response.StatusCode != 404 {
		return nil, errors.Wrapf(err, "failed to get twilio chat user")
	}
	if contactUser == nil {
		_, trace, err := s.restClient.CreateUser(chatUser)
		if trace != nil {
			logHTTP(flows.NewHTTPLog(trace, flows.HTTPStatusFromCode, s.redactor))
		}
		if err != nil {
			return nil, errors.Wrap(err, "failed to create twilio chat user")
		}
	}

	flexChannelParams := &CreateFlexChannelParams{
		FlexFlowSid:          s.restClient.flexFlowSid,
		Identity:             string(contact.UUID()),
		ChatUserFriendlyName: contact.Name(),
		ChatFriendlyName:     contact.Name(),
	}
	newFlexChannel, trace, err := s.restClient.CreateFlexChannel(flexChannelParams)
	if trace != nil {
		logHTTP(flows.NewHTTPLog(trace, flows.HTTPStatusFromCode, s.redactor))
	}
	if err != nil {
		return nil, errors.Wrap(err, "failed to create twilio flex chat channel")
	}

	channelWebhook := &CreateChatChannelWebhookParams{
		ConfigurationUrl:        "https://webhook.site/34e495f6-25e9-4b1e-9629-346054be0d13",
		ConfigurationFilters:    []string{"onMessageSent"},
		ConfigurationMethod:     "POST",
		ConfigurationRetryCount: 1,
		Type:                    "webhook",
	}
	_, trace, err = s.restClient.CreateFlexChannelWebhook(channelWebhook, newFlexChannel.Sid)
	if trace != nil {
		logHTTP(flows.NewHTTPLog(trace, flows.HTTPStatusFromCode, s.redactor))
	}
	if err != nil {
		return nil, errors.Wrap(err, "failed to create channel webhook")
	}

	ticket.SetExternalID(newFlexChannel.TaskSid)
	return ticket, nil
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
