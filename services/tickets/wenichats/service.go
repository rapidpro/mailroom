package wenichats

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"

	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
)

const (
	typeWenichats            = "wenichats"
	configurationProjectAuth = "project_auth"
	configurationSectorUUID  = "sector_uuid"
)

var db *sqlx.DB
var lock = &sync.Mutex{}

func initDB(dbURL string) error {
	if db == nil {
		lock.Lock()
		defer lock.Unlock()
		newDB, err := sqlx.Open("postgres", dbURL)
		if err != nil {
			return errors.Wrapf(err, "unable to open database connection")
		}
		SetDB(newDB)
	}
	return nil
}

func SetDB(newDB *sqlx.DB) {
	db = newDB
}

func init() {
	models.RegisterTicketService(typeWenichats, NewService)
}

type service struct {
	rtConfig   *runtime.Config
	restClient *Client
	ticketer   *flows.Ticketer
	redactor   utils.Redactor
	sectorUUID string
}

func NewService(rtCfg *runtime.Config, httpClient *http.Client, httpRetries *httpx.RetryConfig, ticketer *flows.Ticketer, config map[string]string) (models.TicketService, error) {
	authToken := config[configurationProjectAuth]
	sectorUUID := config[configurationSectorUUID]
	baseURL := rtCfg.WenichatsServiceURL
	if authToken != "" && sectorUUID != "" {

		if err := initDB(rtCfg.DB); err != nil {
			return nil, err
		}

		return &service{
			rtConfig:   rtCfg,
			restClient: NewClient(httpClient, httpRetries, baseURL, authToken),
			ticketer:   ticketer,
			redactor:   utils.NewRedactor(flows.RedactionMask, authToken),
			sectorUUID: sectorUUID,
		}, nil
	}

	return nil, errors.New("missing project_auth or sector_uuid")
}

func (s *service) Open(session flows.Session, topic *flows.Topic, body string, assignee *flows.User, logHTTP flows.HTTPLogCallback) (*flows.Ticket, error) {
	ticket := flows.OpenTicket(s.ticketer, topic, body, assignee)
	contact := session.Contact()

	roomData := &RoomRequest{Contact: &Contact{}}

	if assignee != nil {
		roomData.UserEmail = assignee.Email()
	}

	roomData.Contact.ExternalID = string(contact.UUID())
	roomData.Contact.Name = contact.Name()
	roomData.SectorUUID = s.sectorUUID
	roomData.QueueUUID = string(topic.UUID())

	newRoom, trace, err := s.restClient.CreateRoom(roomData)
	if trace != nil {
		logHTTP(flows.NewHTTPLog(trace, flows.HTTPStatusFromCode, s.redactor))
	}
	if err != nil {
		return nil, errors.Wrap(err, "failed to create wenichats room")
	}

	callbackURL := fmt.Sprintf(
		"https://%s/mr/tickets/types/wenichats/event_callback/%s/%s",
		s.rtConfig.Domain,
		s.ticketer.UUID(),
		ticket.UUID(),
	)

	roomCB := &RoomRequest{CallbackURL: callbackURL}

	//updates room to set callback_url to be able to receive messages
	_, trace, err = s.restClient.UpdateRoom(newRoom.UUID, roomCB)
	if trace != nil {
		logHTTP(flows.NewHTTPLog(trace, flows.HTTPStatusFromCode, s.redactor))
	}
	if err != nil {
		return nil, errors.Wrap(err, "failed to create wenichats room webhook")
	}

	// get messages for history
	after := session.Runs()[0].CreatedOn()
	cx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	msgs, err := models.SelectContactMessages(cx, db, int(contact.ID()), after)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get history messages")
	}

	//send history
	for _, msg := range msgs {
		var direction string
		if msg.Direction() == "I" {
			direction = "incoming"
		} else {
			direction = "outgoing"
		}
		m := &MessageRequest{
			Room:        newRoom.UUID,
			Text:        msg.Text(),
			CreatedOn:   msg.CreatedOn(),
			Attachments: parseMsgAttachments(msg.Attachments()),
			Direction:   direction,
		}
		_, trace, err = s.restClient.CreateMessage(m)
		if trace != nil {
			logHTTP(flows.NewHTTPLog(trace, flows.HTTPStatusFromCode, s.redactor))
		}
		if err != nil {
			return nil, errors.Wrap(err, "error calling wenichats to create a history message")
		}
	}

	ticket.SetExternalID(newRoom.UUID)
	return ticket, nil
}

func parseMsgAttachments(atts []utils.Attachment) []Attachment {
	msgAtts := []Attachment{}
	for _, att := range atts {
		newAtt := Attachment{
			ContentType: att.ContentType(),
			URL:         att.URL(),
		}
		msgAtts = append(msgAtts, newAtt)
	}
	return msgAtts
}

func (s *service) Forward(ticket *models.Ticket, msgUUID flows.MsgUUID, text string, attachments []utils.Attachment, logHTTP flows.HTTPLogCallback) error {
	roomUUID := string(ticket.ExternalID())

	msg := &MessageRequest{
		Room:        roomUUID,
		Attachments: []Attachment{},
		Direction:   "incoming",
	}

	if len(attachments) != 0 {
		for _, attachment := range attachments {
			msg.Attachments = append(msg.Attachments, Attachment{ContentType: attachment.ContentType(), URL: attachment.URL()})
		}
	}

	if strings.TrimSpace(text) != "" {
		msg.Text = text
	}

	_, trace, err := s.restClient.CreateMessage(msg)
	if trace != nil {
		logHTTP(flows.NewHTTPLog(trace, flows.HTTPStatusFromCode, s.redactor))
	}
	if err != nil {
		return errors.Wrap(err, "error send message to wenichats")
	}

	return nil
}

func (s *service) Close(tickets []*models.Ticket, logHTTP flows.HTTPLogCallback) error {
	for _, t := range tickets {
		_, trace, err := s.restClient.CloseRoom(string(t.ExternalID()))
		if trace != nil {
			logHTTP(flows.NewHTTPLog(trace, flows.HTTPStatusFromCode, s.redactor))
		}
		if err != nil {
			return errors.Wrap(err, "error calling wenichats API")
		}
	}
	return nil
}

func (s *service) Reopen(ticket []*models.Ticket, logHTTP flows.HTTPLogCallback) error {
	return errors.New("wenichats ticket type doesn't support reopening")
}
