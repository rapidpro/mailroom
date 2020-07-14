package rocketchat

import (
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/goflow/utils/httpx"
	"github.com/nyaruka/goflow/utils/uuids"
	"github.com/nyaruka/mailroom/models"
	"github.com/pkg/errors"
	"net/http"
)

const (
	typeRocketChat = "rocketchat"

	configDomain = "domain"
	configSecret = "secret"
)

func init() {
	models.RegisterTicketService(typeRocketChat, NewService)
}

type service struct {
	client   *Client
	ticketer *flows.Ticketer
	redactor utils.Redactor
}

func NewService(httpClient *http.Client, httpRetries *httpx.RetryConfig, ticketer *flows.Ticketer, config map[string]string) (models.TicketService, error) {
	domain := config[configDomain]
	secret := config[configSecret]

	if domain != "" && secret != "" {
		return &service{
			client:   NewClient(httpClient, httpRetries, domain, secret),
			ticketer: ticketer,
			redactor: utils.NewRedactor(flows.RedactionMask, secret),
		}, nil
	}
	return nil, errors.New("missing domain or secret config")
}

func (s *service) Open(session flows.Session, subject, body string, logHTTP flows.HTTPLogCallback) (*flows.Ticket, error) {
	contact := session.Contact()
	email := ""
	phone := ""

	for _, urn := range contact.URNs() {
		scheme := urn.URN().Scheme()

		if scheme == urns.EmailScheme {
			email = urn.URN().Path()
		}
		if scheme == urns.TelScheme {
			phone = urn.URN().Path()
		}
		if email != "" && phone != "" {
			break
		}
	}

	ticketUUID := flows.TicketUUID(uuids.New())
	visitor := &Visitor{
		Token: VisitorToken(ticketUUID),
		Name:  contact.Name(),
		Email: email,
		Phone: phone,
	}
	room, trace, err := s.client.CreateRoom(visitor)
	if trace != nil {
		logHTTP(flows.NewHTTPLog(trace, flows.HTTPStatusFromCode, s.redactor))
	}
	if err != nil {
		return nil, err
	}

	return flows.NewTicket(ticketUUID, s.ticketer.Reference(), subject, body, room.ID), nil
}

func (s *service) Forward(ticket *models.Ticket, msgUUID flows.MsgUUID, text string, logHTTP flows.HTTPLogCallback) error {
	msg := &VisitorMsg{Text: text}
	msg.Visitor.Token = VisitorToken(ticket.UUID())

	_, trace, err := s.client.SendMessage(msg)
	if trace != nil {
		logHTTP(flows.NewHTTPLog(trace, flows.HTTPStatusFromCode, s.redactor))
	}
	return err
}

func (s *service) Close(tickets []*models.Ticket, logHTTP flows.HTTPLogCallback) error {
	for _, t := range tickets {
		trace, err := s.client.CloseRoom(VisitorToken(t.UUID()), "")
		if trace != nil {
			logHTTP(flows.NewHTTPLog(trace, flows.HTTPStatusFromCode, s.redactor))
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *service) Reopen(tickets []*models.Ticket, logHTTP flows.HTTPLogCallback) error {
	return errors.New("RocketChat ticketer doesn't support reopening")
}
