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

	"github.com/pkg/errors"
)

func init() {
	web.RegisterJSONRoute(http.MethodPost, "/mr/ticket/zendesk/channelback", handleChannelback)
	web.RegisterJSONRoute(http.MethodPost, "/mr/ticket/zendesk/event_callback", handleEventCallback)
}

type metadata struct {
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
	metadata := &metadata{}
	if err := utils.UnmarshalAndValidate([]byte(request.Metadata), metadata); err != nil {
		return errors.Wrapf(err, "error unmarshaling metadata"), http.StatusBadRequest, nil
	}

	// lookup the ticketer associated with this Zendesk channel
	ticketer, err := models.LookupTicketerByUUID(ctx, s.DB, metadata.TicketerUUID)
	if err != nil {
		return errors.Wrapf(err, "error loading ticketer"), http.StatusBadRequest, nil
	}

	// check secret is correct
	if ticketer.Config("secret") != metadata.Secret {
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

type event struct {
	TypeID          string          `json:"type_id"`
	Timestamp       time.Time       `json:"timestamp"`
	Subdomain       string          `json:"subdomain"`
	IntegrationName string          `json:"integration_name"`
	IntegrationID   string          `json:"integration_id"`
	Error           string          `json:"error"`
	Data            json.RawMessage `json:"data"`
}

type resourceEvent struct {
	TypeID     string `json:"type_id"`
	TicketID   int64  `json:"ticket_id"`
	CommentID  int64  `json:"comment_id"`
	ExternalID string `json:"external_id"`
}

type resourcesCreatedEvent struct {
	RequestID      string          `json:"request_id"`
	ResourceEvents []resourceEvent `json:"resource_events"`
}

type eventCallbackRequest struct {
	Events []*event `json:"events"`
}

func handleEventCallback(ctx context.Context, s *web.Server, r *http.Request) (interface{}, int, error) {
	request := &eventCallbackRequest{}
	if err := utils.UnmarshalAndValidateWithLimit(r.Body, request, web.MaxRequestBytes); err != nil {
		return nil, http.StatusBadRequest, err
	}

	for _, e := range request.Events {
		// TODO lookup ticketer by subdomain.. check integration_id ?

		if e.TypeID == "resources_created_from_external_ids" {
			event := &resourcesCreatedEvent{}
			if err := utils.UnmarshalAndValidate(e.Data, event); err != nil {
				return nil, http.StatusBadRequest, err
			}

			for _, re := range event.ResourceEvents {
				if re.TypeID == "comment_on_new_ticket" {
					// new tickets aren't created from actual messages - the external_id on the "message" we push
					// to Zendesk is actually the UUID of the ticket we just created
					ticket, err := models.LookupTicketByUUID(ctx, s.DB, flows.TicketUUID(re.ExternalID))
					if err != nil {
						return nil, http.StatusBadRequest, err
					}

					// update the ticket with the ID from Zendesk
					models.UpdateTicketExternalID(ctx, s.DB, ticket, fmt.Sprintf("%d", re.TicketID))
				}
			}
		}
	}

	return map[string]string{"status": "OK"}, http.StatusOK, nil
}
