package rocketchat

import (
	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/pkg/errors"
	"net/http"
	"strconv"
	"time"
)

const (
	typeRocketChat = "rocketchat"

	configBaseURL        = "base_url"
	configSecret         = "secret"
	configAdminAuthToken = "admin_auth_token"
	configAdminUserID    = "admin_user_id"
)

func init() {
	models.RegisterTicketService(typeRocketChat, NewService)
}

type service struct {
	client   *Client
	ticketer *flows.Ticketer
	redactor utils.Redactor
}

// NewService creates a new RocketChat ticket service
func NewService(httpClient *http.Client, httpRetries *httpx.RetryConfig, ticketer *flows.Ticketer, config map[string]string) (models.TicketService, error) {
	baseURL := config[configBaseURL]
	secret := config[configSecret]

	if baseURL != "" && secret != "" {
		return &service{
			client:   NewClient(httpClient, httpRetries, baseURL, secret),
			ticketer: ticketer,
			redactor: utils.NewRedactor(flows.RedactionMask, secret),
		}, nil
	}
	return nil, errors.New("missing base_url or secret config")
}

// VisitorToken ticket user ID, RocketChat allows one room/ticket per user/contact
type VisitorToken models.ContactID

// Open opens a ticket which for RocketChat means open a room associated to a visitor user
func (s *service) Open(session flows.Session, subject, body string, logHTTP flows.HTTPLogCallback) (*flows.Ticket, error) {
	contact := session.Contact()
	email := ""
	phone := ""

	// look up email and phone
	for _, urn := range contact.URNs() {
		switch urn.URN().Scheme() {
		case urns.EmailScheme:
			email = urn.URN().Path()
		case urns.TelScheme:
			phone = urn.URN().Path()
		}
		if email != "" && phone != "" {
			break
		}
	}

	ticketUUID := flows.TicketUUID(uuids.New())
	room := &Room{
		Visitor: Visitor{
			Token:       VisitorToken(contact.ID()).String(),
			ContactUUID: string(contact.UUID()),
			Name:        contact.Name(),
			Email:       email,
			Phone:       phone,
		},
		TicketID: string(ticketUUID),
	}
	room.SessionStart = session.Runs()[0].CreatedOn().Add(-time.Minute).Format(time.RFC3339)

	// to fully support the RocketChat ticketer, look up extra fields from ticket body for now
	extra := &struct {
		Department   string            `json:"department"`
		CustomFields map[string]string `json:"customFields"`
	}{}
	if err := jsonx.Unmarshal([]byte(body), extra); err == nil {
		room.Visitor.Department = extra.Department
		room.Visitor.CustomFields = extra.CustomFields
	}

	roomID, trace, err := s.client.CreateRoom(room)
	if trace != nil {
		logHTTP(flows.NewHTTPLog(trace, flows.HTTPStatusFromCode, s.redactor))
	}
	if err != nil {
		return nil, errors.Wrap(err, "error calling RocketChat")
	}

	return flows.NewTicket(ticketUUID, s.ticketer.Reference(), subject, body, roomID), nil
}

func (s *service) Forward(ticket *models.Ticket, msgUUID flows.MsgUUID, text string, attachments []utils.Attachment, logHTTP flows.HTTPLogCallback) error {
	visitor := Visitor{
		Token: VisitorToken(ticket.ContactID()).String(),
	}
	msg := &VisitorMsg{
		Visitor: visitor,
		Text:    text,
	}
	for _, attachment := range attachments {
		mimeType, url := attachment.ToParts()
		msg.Attachments = append(msg.Attachments, Attachment{Type: mimeType, URL: url})
	}

	_, trace, err := s.client.SendMessage(msg)
	if trace != nil {
		logHTTP(flows.NewHTTPLog(trace, flows.HTTPStatusFromCode, s.redactor))
	}
	if err != nil {
		return errors.Wrap(err, "error calling RocketChat")
	}
	return nil
}

func (s *service) Close(tickets []*models.Ticket, logHTTP flows.HTTPLogCallback) error {
	for _, t := range tickets {
		visitor := &Visitor{Token: VisitorToken(t.ContactID()).String()}

		trace, err := s.client.CloseRoom(visitor)
		if trace != nil {
			logHTTP(flows.NewHTTPLog(trace, flows.HTTPStatusFromCode, s.redactor))
		}
		if err != nil {
			return errors.Wrap(err, "error calling RocketChat")
		}
	}
	return nil
}

func (s *service) Reopen(tickets []*models.Ticket, logHTTP flows.HTTPLogCallback) error {
	return errors.New("RocketChat ticket type doesn't support reopening")
}

func (t VisitorToken) String() string {
	return strconv.FormatInt(int64(t), 10)
}
