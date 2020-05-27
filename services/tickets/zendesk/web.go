package zendesk

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/services/tickets"
	"github.com/nyaruka/mailroom/web"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func init() {
	base := "/mr/tickets/types/zendesk"

	web.RegisterJSONRoute(http.MethodPost, base+"/channelback", handleChannelback)
	web.RegisterJSONRoute(http.MethodPost, base+"/event_callback", tickets.WithHTTPLogs(handleEventCallback))
	web.RegisterJSONRoute(http.MethodPost, base+"/ticket_callback", tickets.WithHTTPLogs(handleTicketCallback))
}

type integrationMetadata struct {
	TicketerUUID assets.TicketerUUID `json:"ticketer" validate:"required"`
	Secret       string              `json:"secret"   validate:"required"`
}

type channelbackRequest struct {
	Message     string   `form:"message" validate:"required"`
	FileURLs    []string `form:"file_urls"`
	ParentID    string   `form:"parent_id"`
	ThreadID    string   `form:"thread_id"`
	RecipientID string   `form:"recipient_id" validate:"required"`
	Metadata    string   `form:"metadata" validate:"required"`
}

type channelbackResponse struct {
	ExternalID       string `json:"external_id"`
	AllowChannelback bool   `json:"allow_channelback"`
}

func handleChannelback(ctx context.Context, s *web.Server, r *http.Request) (interface{}, int, error) {
	request := &channelbackRequest{}
	if err := web.DecodeAndValidateForm(request, r); err != nil {
		return errors.Wrapf(err, "error decoding form"), http.StatusBadRequest, nil
	}

	// decode our metadata
	metadata := &integrationMetadata{}
	if err := utils.UnmarshalAndValidate([]byte(request.Metadata), metadata); err != nil {
		return errors.Wrapf(err, "error unmarshaling metadata"), http.StatusBadRequest, nil
	}

	// load our ticket
	ticket, err := models.LookupTicketByUUID(ctx, s.DB, flows.TicketUUID(request.ThreadID))
	if err != nil {
		return errors.Wrapf(err, "error loading ticket"), http.StatusBadRequest, nil
	}

	// and then the ticketer that created it
	ticketer, _, err := tickets.TicketerFromTicket(ctx, s.DB, ticket, typeZendesk)
	if err != nil {
		return err, http.StatusBadRequest, nil
	}

	// check ticketer secret
	if ticketer.Config(configSecret) != metadata.Secret {
		return errors.New("ticketer secret mismatch"), http.StatusBadRequest, nil
	}

	err = models.UpdateAndKeepOpenTicket(ctx, s.DB, ticket, nil)
	if err != nil {
		return errors.Wrapf(err, "error updating ticket: %s", ticket.UUID()), http.StatusBadRequest, nil
	}

	msg, err := tickets.SendReply(ctx, s.DB, s.RP, ticket, request.Message)
	if err != nil {
		return err, http.StatusBadRequest, nil
	}

	return &channelbackResponse{ExternalID: fmt.Sprintf("%d", msg.ID()), AllowChannelback: true}, http.StatusOK, nil
}

type channelEvent struct {
	TypeID          string          `json:"type_id"`
	Timestamp       time.Time       `json:"timestamp"`
	Subdomain       string          `json:"subdomain"`
	IntegrationName string          `json:"integration_name"`
	IntegrationID   string          `json:"integration_id"`
	Error           string          `json:"error"`
	Data            json.RawMessage `json:"data"`
}

type integrationInstanceData struct {
	Metadata string `json:"metadata"`
}

type resourceEvent struct {
	TypeID     string `json:"type_id"`
	TicketID   int64  `json:"ticket_id"`
	CommentID  int64  `json:"comment_id"`
	ExternalID string `json:"external_id"`
}

type resourcesCreatedData struct {
	RequestID      string          `json:"request_id"`
	ResourceEvents []resourceEvent `json:"resource_events"`
}

type eventCallbackRequest struct {
	Events []*channelEvent `json:"events" validate:"required"`
}

func handleEventCallback(ctx context.Context, s *web.Server, r *http.Request, l *models.HTTPLogger) (interface{}, int, error) {
	request := &eventCallbackRequest{}
	if err := utils.UnmarshalAndValidateWithLimit(r.Body, request, web.MaxRequestBytes); err != nil {
		return err, http.StatusBadRequest, nil
	}

	for _, e := range request.Events {
		if err := processChannelEvent(ctx, s.DB, e, l); err != nil {
			return err, http.StatusBadRequest, nil
		}
	}

	return map[string]string{"status": "OK"}, http.StatusOK, nil
}

func processChannelEvent(ctx context.Context, db *sqlx.DB, event *channelEvent, l *models.HTTPLogger) error {
	lr := logrus.WithField("integration_id", event.IntegrationID).WithField("subdomain", event.Subdomain)

	switch event.TypeID {

	case "create_integration":
		lr.Info("zendesk app installed")
	case "destroy_integration":
		lr.Info("zendesk app uninstalled")

	case "create_integration_instance", "destroy_integration_instance":
		data := &integrationInstanceData{}
		if err := utils.UnmarshalAndValidate(event.Data, data); err != nil {
			return err
		}

		metadata := &integrationMetadata{}
		if err := utils.UnmarshalAndValidate([]byte(data.Metadata), metadata); err != nil {
			return errors.Wrapf(err, "error unmarshaling metadata")
		}

		ticketer, svc, err := loadTicketer(ctx, db, metadata.TicketerUUID, metadata.Secret)
		if err != nil {
			return err
		}

		if event.TypeID == "create_integration_instance" {
			// user has added an account through the admin UI
			if err := svc.addStatusCallback(event.IntegrationName, event.IntegrationID, l.Ticketer(ticketer)); err != nil {
				return err
			}

			lr.Info("zendesk channel account added")
		} else {
			// user has removed a channel account
			if err := svc.removeStatusCallback(event.IntegrationName, event.IntegrationID, l.Ticketer(ticketer)); err != nil {
				return err
			}

			lr.Info("zendesk channel account removed")
		}

	case "resources_created_from_external_ids":
		data := &resourcesCreatedData{}
		if err := utils.UnmarshalAndValidate(event.Data, data); err != nil {
			return err
		}

		// parse the request ID we passed to zendesk when we pushed these external resources
		reqID, err := ParseRequestID(data.RequestID)
		if err != nil {
			return err
		}

		for _, re := range data.ResourceEvents {
			if re.TypeID == "comment_on_new_ticket" {
				// look up our ticket
				ticket, err := models.LookupTicketByUUID(ctx, db, flows.TicketUUID(re.ExternalID))
				if err != nil {
					return err
				}

				// and then the ticketer that created it
				ticketer, svc, err := tickets.TicketerFromTicket(ctx, db, ticket, typeZendesk)
				if err != nil {
					return err
				}
				zendesk := svc.(*service)

				// check ticketer secret
				if ticketer.Config(configSecret) != reqID.Secret {
					return errors.New("ticketer secret mismatch")
				}

				// update our local ticket with the ID from Zendesk
				models.UpdateTicketExternalID(ctx, db, ticket, fmt.Sprintf("%d", re.TicketID))

				// update zendesk ticket with our UUID
				zendesk.setExternalID(ticket, l.Ticketer(ticketer))
			}
		}
	}
	return nil
}

type ticketCallbackTicket struct {
	ID         int64  `json:"id"`
	ExternalID string `json:"external_id"`
	Status     string `json:"status"`
	Via        string `json:"via"`
	Link       string `json:"link"`
}

type ticketCallbackRequest struct {
	Event  string               `json:"event"`
	Ticket ticketCallbackTicket `json:"ticket"`
}

func handleTicketCallback(ctx context.Context, s *web.Server, r *http.Request, l *models.HTTPLogger) (interface{}, int, error) {
	request := &ticketCallbackRequest{}
	if err := utils.UnmarshalAndValidateWithLimit(r.Body, request, web.MaxRequestBytes); err != nil {
		return nil, http.StatusBadRequest, err
	}

	// TODO authentication?

	// if this ticket doesn't have an external ID then it doesn't belong to us
	if request.Ticket.ExternalID == "" {
		return map[string]string{"status": "ignored"}, http.StatusOK, nil
	}

	if request.Event == "status_changed" {
		if err := processTicketStatusChange(ctx, s.DB, &request.Ticket, l); err != nil {
			return err, http.StatusBadRequest, nil
		}
	}

	return map[string]string{"status": "handled"}, http.StatusOK, nil
}

func processTicketStatusChange(ctx context.Context, db *sqlx.DB, callback *ticketCallbackTicket, l *models.HTTPLogger) error {
	ticket, err := models.LookupTicketByUUID(ctx, db, flows.TicketUUID(callback.ExternalID))
	if err != nil {
		return err
	}

	switch strings.ToLower(callback.Status) {
	case statusSolved:
		err = models.CloseTickets(ctx, db, nil, []*models.Ticket{ticket}, false, l)
	case statusOpen:
		err = models.ReopenTickets(ctx, db, nil, []*models.Ticket{ticket}, false, l)
	}

	return err
}

func loadTicketer(ctx context.Context, db *sqlx.DB, uuid assets.TicketerUUID, secret string) (*models.Ticketer, *service, error) {
	ticketer, err := models.LookupTicketerByUUID(ctx, db, uuid)
	if err != nil {
		return nil, nil, err
	}

	// check secret
	if ticketer.Config(configSecret) != secret {
		return nil, nil, errors.New("ticketer secret mismatch")
	}

	// and load it as a service
	svc, err := ticketer.AsService(flows.NewTicketer(ticketer))
	if err != nil {
		return nil, nil, errors.Wrap(err, "error loading ticketer service")
	}

	return ticketer, svc.(*service), nil
}
