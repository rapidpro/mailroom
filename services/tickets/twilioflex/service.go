package twilioflex

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/gabriel-vasile/mimetype"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

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
	configurationFlexFlowSid    = "flex_flow_sid"
)

var db *sqlx.DB
var lock = &sync.Mutex{}
var historyDelay = 6

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
	models.RegisterTicketService(typeTwilioFlex, NewService)
}

type service struct {
	rtConfig   *runtime.Config
	restClient *Client
	ticketer   *flows.Ticketer
	redactor   utils.Redactor
}

// newService creates a new twilio flex ticket service
func NewService(rtCfg *runtime.Config, httpClient *http.Client, httpRetries *httpx.RetryConfig, ticketer *flows.Ticketer, config map[string]string) (models.TicketService, error) {
	authToken := config[configurationAuthToken]
	accountSid := config[configurationAccountSid]
	chatServiceSid := config[configurationChatServiceSid]
	workspaceSid := config[configurationWorkspaceSid]
	flexFlowSid := config[configurationFlexFlowSid]
	if authToken != "" && accountSid != "" && chatServiceSid != "" && workspaceSid != "" {

		if err := initDB(rtCfg.DB); err != nil {
			return nil, err
		}

		return &service{
			rtConfig:   rtCfg,
			ticketer:   ticketer,
			restClient: NewClient(httpClient, httpRetries, authToken, accountSid, chatServiceSid, workspaceSid, flexFlowSid),
			redactor:   utils.NewRedactor(flows.RedactionMask, authToken, accountSid, chatServiceSid, workspaceSid),
		}, nil
	}

	return nil, errors.New("missing auth_token or account_sid or chat_service_sid or workspace_sid in twilio flex config")
}

// Open opens a ticket wich for Twilioflex means create a Chat Channel associated to a Chat User
func (s *service) Open(session flows.Session, topic *flows.Topic, body string, assignee *flows.User, logHTTP flows.HTTPLogCallback) (*flows.Ticket, error) {
	ticket := flows.OpenTicket(s.ticketer, topic, body, assignee)
	contact := session.Contact()
	chatUser := &CreateChatUserParams{
		Identity:     fmt.Sprint(contact.ID()),
		FriendlyName: contact.Name(),
	}
	contactUser, trace, err := s.restClient.FetchUser(chatUser.Identity)
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
		Identity:             fmt.Sprint(contact.ID()),
		ChatUserFriendlyName: contact.Name(),
		ChatFriendlyName:     contact.Name(),
	}

	flexChannelParams.TaskAttributes = body

	bodyStruct := struct {
		FlexFlowSid *string `json:"flex_flow_sid,omitempty"`
	}{}

	json.Unmarshal([]byte(body), &bodyStruct)

	if bodyStruct.FlexFlowSid != nil {
		flexChannelParams.FlexFlowSid = *bodyStruct.FlexFlowSid
	}

	newFlexChannel, trace, err := s.restClient.CreateFlexChannel(flexChannelParams)
	if trace != nil {
		logHTTP(flows.NewHTTPLog(trace, flows.HTTPStatusFromCode, s.redactor))
	}
	if err != nil {
		return nil, errors.Wrap(err, "failed to create twilio flex chat channel")
	}

	callbackURL := fmt.Sprintf(
		"https://%s/mr/tickets/types/twilioflex/event_callback/%s/%s",
		s.rtConfig.Domain,
		s.ticketer.UUID(),
		ticket.UUID(),
	)

	channelWebhook := &CreateChatChannelWebhookParams{
		ConfigurationUrl:        callbackURL,
		ConfigurationFilters:    []string{"onMessageSent", "onChannelUpdated", "onMediaMessageSent"},
		ConfigurationMethod:     "POST",
		ConfigurationRetryCount: 0,
		Type:                    "webhook",
	}
	_, trace, err = s.restClient.CreateFlexChannelWebhook(channelWebhook, newFlexChannel.Sid)
	if trace != nil {
		logHTTP(flows.NewHTTPLog(trace, flows.HTTPStatusFromCode, s.redactor))
	}
	if err != nil {
		return nil, errors.Wrap(err, "failed to create channel webhook")
	}

	go func() {
		time.Sleep(time.Second * time.Duration(historyDelay))
		SendHistory(session, contact.ID(), newFlexChannel, logHTTP, s.restClient, s.redactor)
	}()

	ticket.SetExternalID(newFlexChannel.Sid)
	return ticket, nil
}

func (s *service) Forward(ticket *models.Ticket, msgUUID flows.MsgUUID, text string, attachments []utils.Attachment, logHTTP flows.HTTPLogCallback) error {
	identity := fmt.Sprint(ticket.ContactID())

	if len(attachments) > 0 {
		mediaAttachements := []CreateMediaParams{}
		for _, attachment := range attachments {
			attUrl := attachment.URL()
			req, err := http.NewRequest("GET", attUrl, nil)
			if err != nil {
				return err
			}
			resp, err := httpx.DoTrace(s.restClient.httpClient, req, s.restClient.httpRetries, nil, -1)
			if err != nil {
				return err
			}

			parsedURL, err := url.Parse(attUrl)
			if err != nil {
				return err
			}
			filename := path.Base(parsedURL.Path)

			mimeType := mimetype.Detect(resp.ResponseBody)

			media := CreateMediaParams{
				FileName:    filename,
				Media:       resp.ResponseBody,
				Author:      identity,
				ContentType: mimeType.String(),
			}

			mediaAttachements = append(mediaAttachements, media)
		}

		for _, mediaParams := range mediaAttachements {
			media, trace, err := s.restClient.CreateMedia(&mediaParams)
			if trace != nil {
				logHTTP(flows.NewHTTPLog(trace, flows.HTTPStatusFromCode, s.redactor))
			}
			if err != nil {
				return err
			}

			msg := &CreateChatMessageParams{
				From:       identity,
				ChannelSid: string(ticket.ExternalID()),
				MediaSid:   media.Sid,
			}
			_, trace, err = s.restClient.CreateMessage(msg, http.Header{"X-Twilio-Webhook-Enabled": []string{"True"}})
			if trace != nil {
				logHTTP(flows.NewHTTPLog(trace, flows.HTTPStatusFromCode, s.redactor))
			}
			if err != nil {
				return err
			}
		}

	}

	if strings.TrimSpace(text) != "" {
		msg := &CreateChatMessageParams{
			From:       identity,
			Body:       text,
			ChannelSid: string(ticket.ExternalID()),
		}
		_, trace, err := s.restClient.CreateMessage(msg, http.Header{"X-Twilio-Webhook-Enabled": []string{"True"}})
		if trace != nil {
			logHTTP(flows.NewHTTPLog(trace, flows.HTTPStatusFromCode, s.redactor))
		}
		if err != nil {
			return errors.Wrap(err, "error calling Twilio")
		}
	}

	return nil
}

func (s *service) Close(tickets []*models.Ticket, logHTTP flows.HTTPLogCallback) error {
	for _, t := range tickets {
		flexChannel, trace, err := s.restClient.FetchFlexChannel(string(t.ExternalID()))
		if trace != nil {
			logHTTP(flows.NewHTTPLog(trace, flows.HTTPStatusFromCode, s.redactor))
		}
		if err != nil {
			return errors.Wrap(err, "error calling Twilio API")
		}

		_, trace, err = s.restClient.CompleteTask(flexChannel.TaskSid)
		if trace != nil {
			logHTTP(flows.NewHTTPLog(trace, flows.HTTPStatusFromCode, s.redactor))
		}
		if err != nil {
			return errors.Wrap(err, "error calling Twilio API")
		}
	}
	return nil
}

func (s *service) Reopen(tickets []*models.Ticket, logHTTP flows.HTTPLogCallback) error {
	return errors.New("Twilio Flex ticket type doesn't support reopening")
}

func SendHistory(session flows.Session, contactID flows.ContactID, newFlexChannel *FlexChannel, logHTTP flows.HTTPLogCallback, restClient *Client, redactor utils.Redactor) {
	after := session.Runs()[0].CreatedOn()
	cx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	// get messages for history
	msgs, err := models.SelectContactMessages(cx, db, int(contactID), after)
	if err != nil {
		logrus.Error(errors.Wrap(err, "failed to get history messages"))
		return
	}
	var trace *httpx.Trace
	// send history
	for _, msg := range msgs {
		m := &CreateChatMessageParams{
			Body:        msg.Text(),
			ChannelSid:  newFlexChannel.Sid,
			DateCreated: msg.CreatedOn().Format(time.RFC3339),
		}
		if msg.Direction() == "I" {
			m.From = fmt.Sprint(contactID)
			headerWebhookEnabled := http.Header{"X-Twilio-Webhook-Enabled": []string{"True"}}
			_, trace, err = restClient.CreateMessage(m, headerWebhookEnabled)
		} else {
			m.From = "Bot"
			_, trace, err = restClient.CreateMessage(m, nil)
		}
		if trace != nil {
			logHTTP(flows.NewHTTPLog(trace, flows.HTTPStatusFromCode, redactor))
		}
		if err != nil {
			logrus.Error(errors.Wrap(err, "error calling Twilio to send message from history"))
			return
		}
	}
}
