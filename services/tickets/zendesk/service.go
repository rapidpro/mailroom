package zendesk

import (
	"fmt"
	"net/http"

	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/goflow/utils/uuids"
	"github.com/nyaruka/mailroom/models"

	"github.com/pkg/errors"
)

const (
	typeZendesk = "zendesk"

	configSubdomain  = "subdomain"
	configSecret     = "secret"
	configOAuthToken = "oauth_token"
	configPushID     = "push_id"
	configPushToken  = "push_token"
	configTargetID   = "target_id"
	configTriggerID  = "trigger_id"

	statusOpen   = "open"
	statusSolved = "solved"
	statusClosed = "closed"
)

func init() {
	models.RegisterTicketService(typeZendesk, NewService)
}

type service struct {
	restClient     *RESTClient
	pushClient     *PushClient
	ticketer       *flows.Ticketer
	redactor       utils.Redactor
	secret         string
	instancePushID string
	targetID       string
	triggerID      string
}

// NewService creates a new zendesk ticket service
func NewService(httpClient *http.Client, httpRetries *httpx.RetryConfig, ticketer *flows.Ticketer, config map[string]string) (models.TicketService, error) {
	subdomain := config[configSubdomain]
	secret := config[configSecret]
	oAuthToken := config[configOAuthToken]
	instancePushID := config[configPushID]
	pushToken := config[configPushToken]
	targetID := config[configTargetID]
	triggerID := config[configTriggerID]

	if subdomain != "" && secret != "" && oAuthToken != "" && instancePushID != "" && pushToken != "" {
		return &service{
			restClient:     NewRESTClient(httpClient, httpRetries, subdomain, oAuthToken),
			pushClient:     NewPushClient(httpClient, httpRetries, subdomain, pushToken),
			ticketer:       ticketer,
			redactor:       utils.NewRedactor(flows.RedactionMask, oAuthToken, pushToken),
			secret:         secret,
			instancePushID: instancePushID,
			targetID:       targetID,
			triggerID:      triggerID,
		}, nil
	}
	return nil, errors.New("missing subdomain or secret or oauth_token or push_id or push_token in zendesk config")
}

// Open opens a ticket which for mailgun means just sending an initial email
func (s *service) Open(session flows.Session, subject, body string, logHTTP flows.HTTPLogCallback) (*flows.Ticket, error) {
	ticketUUID := flows.TicketUUID(uuids.New())
	contactDisplay := session.Contact().Format(session.Environment())

	msg := &ExternalResource{
		ExternalID: string(ticketUUID), // there's no local msg so use ticket UUID instead
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

func (s *service) Forward(ticket *models.Ticket, msgUUID flows.MsgUUID, text string, logHTTP flows.HTTPLogCallback) error {
	contactUUID := ticket.Config("contact-uuid")
	contactDisplay := ticket.Config("contact-display")

	msg := &ExternalResource{
		ExternalID: string(msgUUID),
		Message:    text,
		ThreadID:   string(ticket.UUID()),
		CreatedAt:  dates.Now(),
		Author: Author{
			ExternalID: contactUUID,
			Name:       contactDisplay,
		},
		AllowChannelback: true,
	}

	return s.push(msg, logHTTP)
}

func (s *service) Close(tickets []*models.Ticket, logHTTP flows.HTTPLogCallback) error {
	ids, err := ticketsToZendeskIDs(tickets)
	if err != nil {
		return nil
	}

	_, trace, err := s.restClient.UpdateManyTickets(ids, statusClosed)
	if trace != nil {
		logHTTP(flows.NewHTTPLog(trace, flows.HTTPStatusFromCode, s.redactor))
	}
	return err
}

func (s *service) Reopen(tickets []*models.Ticket, logHTTP flows.HTTPLogCallback) error {
	ids, err := ticketsToZendeskIDs(tickets)
	if err != nil {
		return nil
	}

	_, trace, err := s.restClient.UpdateManyTickets(ids, statusOpen)
	if trace != nil {
		logHTTP(flows.NewHTTPLog(trace, flows.HTTPStatusFromCode, s.redactor))
	}
	return err
}

// AddStatusCallback adds a target and trigger to callback to us when ticket status is changed
func (s *service) AddStatusCallback(name, domain string, logHTTP flows.HTTPLogCallback) (map[string]string, error) {
	targetURL := fmt.Sprintf("https://%s/mr/tickets/types/zendesk/target/%s", domain, s.ticketer.UUID())

	target := &Target{
		Type:        "http_target",
		Title:       fmt.Sprintf("%s Tickets", name),
		TargetURL:   targetURL,
		Method:      "POST",
		Username:    "zendesk",
		Password:    s.secret,
		ContentType: "application/json",
	}

	target, trace, err := s.restClient.CreateTarget(target)
	if trace != nil {
		logHTTP(flows.NewHTTPLog(trace, flows.HTTPStatusFromCode, s.redactor))
	}
	if err != nil {
		return nil, err
	}

	payload := `{
	"event": "status_changed",
	"id": {{ticket.id}},
	"status": "{{ticket.status}}"
}`

	trigger := &Trigger{
		Title: fmt.Sprintf("Notify %s on ticket status change", name),
		Conditions: Conditions{
			All: []Condition{
				{Field: "status", Operator: "changed"},
				{Field: "via_id", Operator: "is", Value: "55"}, // see https://developer.zendesk.com/rest_api/docs/support/triggers#via-types
			},
		},
		Actions: []Action{
			{Field: "notification_target", Value: []string{fmt.Sprintf("%d", target.ID), string(payload)}},
		},
	}

	trigger, trace, err = s.restClient.CreateTrigger(trigger)
	if trace != nil {
		logHTTP(flows.NewHTTPLog(trace, flows.HTTPStatusFromCode, s.redactor))
	}
	if err != nil {
		return nil, err
	}

	return map[string]string{
		configTargetID:  NumericIDToString(target.ID),
		configTriggerID: NumericIDToString(trigger.ID),
	}, nil
}

func (s *service) RemoveStatusCallback(logHTTP flows.HTTPLogCallback) error {
	if s.triggerID != "" {
		id, _ := ParseNumericID(s.triggerID)
		trace, err := s.restClient.DeleteTrigger(id)
		if trace != nil {
			logHTTP(flows.NewHTTPLog(trace, flows.HTTPStatusFromCode, s.redactor))
		}
		if err != nil {
			return err
		}
	}
	if s.targetID != "" {
		id, _ := ParseNumericID(s.targetID)
		trace, err := s.restClient.DeleteTarget(id)
		if trace != nil {
			logHTTP(flows.NewHTTPLog(trace, flows.HTTPStatusFromCode, s.redactor))
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *service) push(msg *ExternalResource, logHTTP flows.HTTPLogCallback) error {
	rid := NewRequestID(s.secret)

	results, trace, err := s.pushClient.Push(s.instancePushID, rid.String(), []*ExternalResource{msg})
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
