package zendesk

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
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
	web.RegisterJSONRoute(http.MethodPost, base+"/event_callback", handleEventCallback)
	web.RegisterJSONRoute(http.MethodPost, base+"/ticket_callback", handleTicketCallback)
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

	// lookup the ticketer associated with this Zendesk channel
	ticketer, err := models.LookupTicketerByUUID(ctx, s.DB, metadata.TicketerUUID)
	if err != nil {
		return errors.Wrapf(err, "error loading ticketer"), http.StatusBadRequest, nil
	}

	// check secret is correct
	if ticketer.Config(configSecret) != metadata.Secret {
		return errors.Wrapf(err, "ticketer secret mismatch"), http.StatusUnauthorized, nil
	}

	ticket, err := models.LookupTicketByUUID(ctx, s.DB, flows.TicketUUID(request.ThreadID))
	if err != nil {
		return errors.Wrapf(err, "error loading ticket"), http.StatusBadRequest, nil
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

func handleEventCallback(ctx context.Context, s *web.Server, r *http.Request) (interface{}, int, error) {
	logger := &models.HTTPLogger{}

	request := &eventCallbackRequest{}
	if err := utils.UnmarshalAndValidateWithLimit(r.Body, request, web.MaxRequestBytes); err != nil {
		return err, http.StatusBadRequest, nil
	}

	for _, e := range request.Events {
		if err := processChannelEvent(ctx, s.DB, e, logger); err != nil {
			return err, http.StatusBadRequest, nil
		}
	}

	if err := logger.Insert(ctx, s.DB); err != nil {
		return nil, http.StatusInternalServerError, errors.Wrap(err, "error writing HTTP logs")
	}

	return map[string]string{"status": "OK"}, http.StatusOK, nil
}

func processChannelEvent(ctx context.Context, db *sqlx.DB, event *channelEvent, logger *models.HTTPLogger) error {
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

		ticketer, err := models.LookupTicketerByUUID(ctx, db, metadata.TicketerUUID)
		if err != nil {
			return err
		}

		if ticketer.Config(configSecret) != metadata.Secret {
			return errors.New("secret mismatch for ticketer")
		}

		// and load it as a service
		svc, err := ticketer.AsService(flows.NewTicketer(ticketer))
		if err != nil {
			return errors.Wrap(err, "error loading ticketer service")
		}
		zendesk := svc.(*service)

		if event.TypeID == "create_integration_instance" {
			// user has added an account through the admin UI
			if err := zendesk.addCloseCallback(event.IntegrationName, event.IntegrationID, logger.Ticketer(ticketer)); err != nil {
				return err
			}

			lr.Info("zendesk channel account added")
		} else {
			// user has removed a channel account
			if err := zendesk.removeCloseCallback(event.IntegrationName, event.IntegrationID, logger.Ticketer(ticketer)); err != nil {
				return err
			}

			lr.Info("zendesk channel account removed")
		}

	case "resources_created_from_external_ids":
		data := &resourcesCreatedData{}
		if err := utils.UnmarshalAndValidate(event.Data, data); err != nil {
			return err
		}

		// TODO lookup ticketer by subdomain.. check integration_id ?

		for _, re := range data.ResourceEvents {
			if re.TypeID == "comment_on_new_ticket" {
				// new tickets aren't created from actual messages - the external_id on the "message" we push
				// to Zendesk is actually the UUID of the ticket we just created
				ticket, err := models.LookupTicketByUUID(ctx, db, flows.TicketUUID(re.ExternalID))
				if err != nil {
					return err
				}

				// update the ticket with the ID from Zendesk
				models.UpdateTicketExternalID(ctx, db, ticket, fmt.Sprintf("%d", re.TicketID))

				// TODO update external ID on zendesk side to use to handle ticket changes there
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

func handleTicketCallback(ctx context.Context, s *web.Server, r *http.Request) (interface{}, int, error) {
	request := &ticketCallbackRequest{}
	if err := utils.UnmarshalAndValidateWithLimit(r.Body, request, web.MaxRequestBytes); err != nil {
		return nil, http.StatusBadRequest, err
	}

	// TODO authentication?

	if request.Event == "status_changed" && request.Ticket.Status == "Solved" {
		// TODO zendesk IDs aren't unique.. combine with subdomain.. update zendesk tickets to have your ticket UUID as external ID?

		// models.CloseTickets(ctx, s.DB, false)
	}

	return map[string]string{"status": "OK"}, http.StatusOK, nil
}
