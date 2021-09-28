package mailgun

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"
	"text/template"

	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/services/tickets"

	"github.com/pkg/errors"
)

const (
	typeMailgun = "mailgun"

	configDomain    = "domain"
	configAPIKey    = "api_key"
	configToAddress = "to_address"
	configBrandName = "brand_name"
	configURLBase   = "url_base"

	ticketConfigContactUUID    = "contact-uuid"
	ticketConfigContactDisplay = "contact-display"
	ticketConfigLastMessageID  = "last-message-id"
)

// body template for new ticket being opened
var openBodyTemplate = newTemplate("open_body", `New ticket opened
------------------------------------------------

{{.body}}

------------------------------------------------
* Reply to the contact by replying to this email
* Close this ticket by replying with CLOSE
* View this contact at {{.contact_url}}
`)

// body template for message being forwarded from contact
var forwardBodyTemplate = newTemplate("forward_body", `{{.contact}} replied:
------------------------------------------------

{{.message}}

------------------------------------------------
* Reply to the contact by replying to this email
* Close this ticket by replying with CLOSE
* View this contact at {{.contact_url}}
`)

// body template for ticket being closed
var closedBodyTemplate = newTemplate("closed_body", `{{.message}}
* Ticket has been closed
* Replying to the contact will reopen this ticket
* View this contact at {{.contact_url}}
`)

// body template for ticket being reopened
var reopenedBodyTemplate = newTemplate("reopened_body", `{{.message}}
* Ticket has been reopened
* Close this ticket by replying with CLOSE
* View this contact at {{.contact_url}}
`)

func init() {
	models.RegisterTicketService(typeMailgun, NewService)
}

type service struct {
	client    *Client
	ticketer  *flows.Ticketer
	toAddress string
	brandName string
	urlBase   string
	redactor  utils.Redactor
}

// NewService creates a new mailgun email-based ticket service
func NewService(rtCfg *runtime.Config, httpClient *http.Client, httpRetries *httpx.RetryConfig, ticketer *flows.Ticketer, config map[string]string) (models.TicketService, error) {
	domain := config[configDomain]
	apiKey := config[configAPIKey]
	toAddress := config[configToAddress]
	brandName := config[configBrandName]
	urlBase := config[configURLBase]

	if domain != "" && apiKey != "" && toAddress != "" && urlBase != "" {
		// need to redact the string used for basic auth
		basicAuth := base64.StdEncoding.EncodeToString([]byte("api:" + apiKey))

		return &service{
			client:    NewClient(httpClient, httpRetries, domain, apiKey),
			ticketer:  ticketer,
			toAddress: toAddress,
			brandName: brandName,
			urlBase:   urlBase,
			redactor:  utils.NewRedactor(flows.RedactionMask, apiKey, basicAuth),
		}, nil
	}
	return nil, errors.New("missing domain or api_key or to_address or url_base in mailgun config")
}

// Open opens a ticket which for mailgun means just sending an initial email
func (s *service) Open(session flows.Session, topic *flows.Topic, body string, assignee *flows.User, logHTTP flows.HTTPLogCallback) (*flows.Ticket, error) {
	ticket := flows.OpenTicket(s.ticketer, topic, body, assignee)
	contactDisplay := tickets.GetContactDisplay(session.Environment(), session.Contact())

	from := s.ticketAddress(contactDisplay, ticket.UUID())
	context := s.templateContext(body, "", string(session.Contact().UUID()), contactDisplay)
	fullBody := evaluateTemplate(openBodyTemplate, context)

	msgID, trace, err := s.client.SendMessage(from, s.toAddress, subjectFromBody(body), fullBody, nil, nil)
	if trace != nil {
		logHTTP(flows.NewHTTPLog(trace, flows.HTTPStatusFromCode, s.redactor))
	}
	if err != nil {
		return nil, errors.Wrap(err, "error calling mailgun API")
	}

	ticket.SetExternalID(msgID)
	return ticket, nil
}

func (s *service) Forward(ticket *models.Ticket, msgUUID flows.MsgUUID, text string, attachments []utils.Attachment, logHTTP flows.HTTPLogCallback) error {
	context := s.templateContext(ticket.Body(), text, ticket.Config(ticketConfigContactUUID), ticket.Config(ticketConfigContactDisplay))
	body := evaluateTemplate(forwardBodyTemplate, context)

	_, err := s.sendInTicket(ticket, body, attachments, logHTTP)
	return err
}

func (s *service) Close(tickets []*models.Ticket, logHTTP flows.HTTPLogCallback) error {
	for _, ticket := range tickets {
		context := s.templateContext(ticket.Body(), "", ticket.Config(ticketConfigContactUUID), ticket.Config(ticketConfigContactDisplay))
		body := evaluateTemplate(closedBodyTemplate, context)

		_, err := s.sendInTicket(ticket, body, nil, logHTTP)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *service) Reopen(tickets []*models.Ticket, logHTTP flows.HTTPLogCallback) error {
	for _, ticket := range tickets {
		context := s.templateContext(ticket.Body(), "", ticket.Config(ticketConfigContactUUID), ticket.Config(ticketConfigContactDisplay))
		body := evaluateTemplate(reopenedBodyTemplate, context)

		_, err := s.sendInTicket(ticket, body, nil, logHTTP)
		if err != nil {
			return err
		}
	}
	return nil
}

// sends an email as part of the thread for the given ticket
func (s *service) sendInTicket(ticket *models.Ticket, text string, attachments []utils.Attachment, logHTTP flows.HTTPLogCallback) (string, error) {
	contactDisplay := ticket.Config(ticketConfigContactDisplay)
	lastMessageID := ticket.Config(ticketConfigLastMessageID)
	if lastMessageID == "" {
		lastMessageID = string(ticket.ExternalID()) // id of first message sent becomes external ID
	}
	headers := map[string]string{
		"In-Reply-To": lastMessageID,
		"References":  lastMessageID,
	}
	from := s.ticketAddress(contactDisplay, ticket.UUID())

	return s.send(from, s.toAddress, subjectFromBody(ticket.Body()), text, attachments, headers, logHTTP)
}

func (s *service) send(from, to, subject, text string, attachments []utils.Attachment, headers map[string]string, logHTTP flows.HTTPLogCallback) (string, error) {
	// fetch our attachments and convert to email attachments
	emailAttachments := make([]*EmailAttachment, len(attachments))
	for i, attachment := range attachments {
		file, err := tickets.FetchFile(attachment.URL(), nil)
		if err != nil {
			return "", errors.Wrapf(err, "error fetching attachment file")
		}
		emailAttachments[i] = &EmailAttachment{Filename: "untitled", ContentType: file.ContentType, Body: file.Body}
	}

	msgID, trace, err := s.client.SendMessage(from, to, subject, text, emailAttachments, headers)
	if trace != nil {
		logHTTP(flows.NewHTTPLog(trace, flows.HTTPStatusFromCode, s.redactor))
	}
	if err != nil {
		return "", errors.Wrap(err, "error calling mailgun API")
	}

	return msgID, nil
}

func (s *service) ticketAddress(contactDisplay string, ticketUUID flows.TicketUUID) string {
	address := fmt.Sprintf("ticket+%s@%s", ticketUUID, s.client.domain)
	return fmt.Sprintf("%s via %s <%s>", contactDisplay, s.brandName, address)
}

func (s *service) noReplyAddress() string {
	return fmt.Sprintf("no-reply@%s", s.client.domain)
}

func (s *service) templateContext(body, message, contactUUID, contactDisplay string) map[string]string {
	return map[string]string{
		"brand":       s.brandName,                                                // rapidpro brand
		"subject":     subjectFromBody(body),                                      // portion of body used as subject
		"body":        body,                                                       // original ticket body
		"message":     message,                                                    // new message if this is a forward
		"contact":     contactDisplay,                                             // display name contact
		"contact_url": fmt.Sprintf("%s/contact/read/%s/", s.urlBase, contactUUID), // link to contact
	}
}

func newTemplate(name, value string) *template.Template {
	return template.Must(template.New(name).Parse(value))
}

func evaluateTemplate(t *template.Template, c map[string]string) string {
	b := &strings.Builder{}
	t.Execute(b, c)
	return b.String()
}

func subjectFromBody(body string) string {
	return utils.Truncate(strings.ReplaceAll(body, "\n", ""), 64)
}
