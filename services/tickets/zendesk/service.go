package zendesk

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/goflow/utils/dates"
	"github.com/nyaruka/goflow/utils/httpx"
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

	statusOpen   = "open"
	statusSolved = "solved"
)

func init() {
	models.RegisterTicketService(typeZendesk, NewService)
}

type service struct {
	restClient     *RESTClient
	pushClient     *PushClient
	ticketer       *flows.Ticketer
	redactor       utils.Redactor
	instancePushID string
}

// NewService creates a new zendesk ticket service
func NewService(httpClient *http.Client, httpRetries *httpx.RetryConfig, ticketer *flows.Ticketer, config map[string]string) (models.TicketService, error) {
	subdomain := config[configSubdomain]
	oAuthToken := config[configOAuthToken]
	instancePushID := config[configPushID]
	pushToken := config[configPushToken]
	if subdomain != "" && oAuthToken != "" && instancePushID != "" && pushToken != "" {
		return &service{
			restClient:     NewRESTClient(httpClient, httpRetries, subdomain, oAuthToken),
			pushClient:     NewPushClient(httpClient, httpRetries, subdomain, pushToken),
			ticketer:       ticketer,
			redactor:       utils.NewRedactor(flows.RedactionMask, oAuthToken, pushToken),
			instancePushID: instancePushID,
		}, nil
	}
	return nil, errors.New("missing subdomain or oauth_token or push_id or push_token in zendesk config")
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
		DisplayInfo: []DisplayInfo{
			{
				// need to also include our ticket UUID in here so the Zendesk app can access it
				Type: "temba",
				Data: map[string]string{"uuid": string(ticketUUID)},
			},
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
		DisplayInfo: []DisplayInfo{
			{
				Type: "temba-ticket",
				Data: map[string]string{"uuid": string(ticket.UUID())},
			},
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

	_, trace, err := s.restClient.UpdateManyTickets(ids, statusSolved)
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

func (s *service) addCloseCallback(name, domain string, logHTTP flows.HTTPLogCallback) error {
	targetURL := fmt.Sprintf("https://%s/mr/tickets/types/zendesk/ticket_callback", domain)

	// TODO check for existing target with this URL

	target := &Target{
		Type:        "http_target",
		Title:       fmt.Sprintf("%s Tickets", name),
		TargetURL:   targetURL,
		Method:      "POST",
		ContentType: "application/json",
	}

	target, trace, err := s.restClient.CreateTarget(target)
	if trace != nil {
		logHTTP(flows.NewHTTPLog(trace, flows.HTTPStatusFromCode, s.redactor))
	}
	if err != nil {
		return err
	}

	payload := `{
		"event": "status_changed",
		"ticket": {
			"id": {{ticket.id}},
			"external_id": "{{ticket.external_id}}",
			"status": "{{ticket.status}}",
			"via": "{{ticket.via}}",
			"link": "{{ticket.link}}"
		}
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
	return err
}

func (s *service) removeCloseCallback(name, domain string, logHTTP flows.HTTPLogCallback) error {
	// TODO.. check if we're the last ticketer using this integration and then remove?
	return nil
}

func (s *service) push(msg *ExternalResource, logHTTP flows.HTTPLogCallback) error {
	results, trace, err := s.pushClient.Push(s.instancePushID, "", []*ExternalResource{msg})
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

// parses out the zendesk ticket IDs from our external ID field
func ticketsToZendeskIDs(tickets []*models.Ticket) ([]int64, error) {
	ids := make([]int64, len(tickets))
	for i := range tickets {
		idStr := string(tickets[i].ExternalID())
		id, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil {
			return nil, errors.Errorf("%s is not a valid zendesk ticket id", idStr)
		}
		ids[i] = id
	}
	return ids, nil
}
