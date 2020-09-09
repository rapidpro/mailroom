package zendesk

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi"
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
	web.RegisterJSONRoute(http.MethodPost, base+"/event_callback", web.WithHTTPLogs(handleEventCallback))
	web.RegisterJSONRoute(http.MethodPost, base+`/target/{ticketer:[a-f0-9\-]+}`, web.WithHTTPLogs(handleTicketerTarget))
}

type integrationMetadata struct {
	TicketerUUID assets.TicketerUUID `json:"ticketer" validate:"required"`
	Secret       string              `json:"secret"   validate:"required"`
}

type channelbackRequest struct {
	Message     string   `form:"message"      validate:"required"`
	FileURLs    []string `form:"file_urls[]"`
	ParentID    string   `form:"parent_id"`
	ThreadID    string   `form:"thread_id"    validate:"required"`
	RecipientID string   `form:"recipient_id" validate:"required"`
	Metadata    string   `form:"metadata"     validate:"required"`
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

	// lookup the ticket and ticketer
	ticket, ticketer, _, err := tickets.FromTicketUUID(ctx, s.DB, flows.TicketUUID(request.ThreadID), typeZendesk)
	if err != nil {
		return err, http.StatusBadRequest, nil
	}

	// check ticketer secret
	if ticketer.Config(configSecret) != metadata.Secret {
		return errors.New("ticketer secret mismatch"), http.StatusUnauthorized, nil
	}

	err = models.UpdateAndKeepOpenTicket(ctx, s.DB, ticket, nil)
	if err != nil {
		return errors.Wrapf(err, "error updating ticket: %s", ticket.UUID()), http.StatusBadRequest, nil
	}

	msg, err := tickets.SendReply(ctx, s.DB, s.RP, s.Storage, ticket, request.Message, request.FileURLs)
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

		// look up our ticketer
		ticketer, svc, err := tickets.FromTicketerUUID(ctx, db, metadata.TicketerUUID, typeZendesk)
		if err != nil {
			return err
		}
		zendesk := svc.(*service)

		// check secret
		if ticketer.Config(configSecret) != metadata.Secret {
			return errors.New("ticketer secret mismatch")
		}

		if event.TypeID == "create_integration_instance" {
			// user has added an account through the admin UI
			newConfig, err := zendesk.AddStatusCallback(event.IntegrationName, event.IntegrationID, l.Ticketer(ticketer))
			if err != nil {
				return err
			}

			// save away the target and trigger zendesk ids
			if err := ticketer.UpdateConfig(ctx, db, newConfig, nil); err != nil {
				return errors.Wrapf(err, "error updating config for ticketer %s", ticketer.UUID())
			}

			lr.Info("zendesk channel account added")
		} else {
			// user has removed a channel account
			if err := zendesk.RemoveStatusCallback(l.Ticketer(ticketer)); err != nil {
				return err
			}

			// delete config values that came from adding this account
			remConfig := utils.StringSet([]string{configPushID, configPushToken, configTargetID, configTriggerID})
			if err := ticketer.UpdateConfig(ctx, db, nil, remConfig); err != nil {
				return errors.Wrapf(err, "error updating config for ticketer %s", ticketer.UUID())
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
				if err := processCommentOnNewTicket(ctx, db, reqID, re, l); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func processCommentOnNewTicket(ctx context.Context, db *sqlx.DB, reqID RequestID, re resourceEvent, l *models.HTTPLogger) error {
	// look up our ticket and ticketer
	ticket, ticketer, _, err := tickets.FromTicketUUID(ctx, db, flows.TicketUUID(re.ExternalID), typeZendesk)
	if err != nil {
		return err
	}

	// check ticketer secret
	if ticketer.Config(configSecret) != reqID.Secret {
		return errors.New("ticketer secret mismatch")
	}

	// update our local ticket with the ID from Zendesk
	return models.UpdateTicketExternalID(ctx, db, ticket, fmt.Sprintf("%d", re.TicketID))
}

type targetRequest struct {
	Event  string `json:"event"   validate:"required"`
	ID     int64  `json:"id"      validate:"required"`
	Status string `json:"status"`
}

func handleTicketerTarget(ctx context.Context, s *web.Server, r *http.Request, l *models.HTTPLogger) (interface{}, int, error) {
	ticketerUUID := assets.TicketerUUID(chi.URLParam(r, "ticketer"))

	// look up our ticketer
	ticketer, _, err := tickets.FromTicketerUUID(ctx, s.DB, ticketerUUID, typeZendesk)
	if err != nil || ticketer == nil {
		return errors.Errorf("no such ticketer %s", ticketerUUID), http.StatusNotFound, nil
	}

	// check authentication
	username, password, _ := r.BasicAuth()
	if username != "zendesk" || password != ticketer.Config(configSecret) {
		return map[string]string{"status": "unauthorized"}, http.StatusUnauthorized, nil
	}

	// parse request payload
	request := &targetRequest{}
	if err := utils.UnmarshalAndValidateWithLimit(r.Body, request, web.MaxRequestBytes); err != nil {
		return err, http.StatusBadRequest, nil
	}

	// lookup ticket
	ticket, err := models.LookupTicketByExternalID(ctx, s.DB, ticketer.ID(), fmt.Sprintf("%d", request.ID))
	if err != nil || ticket == nil {
		// we don't return an error here, because ticket might just belong to a different ticketer
		return map[string]string{"status": "ignored"}, http.StatusOK, nil
	}

	if request.Event == "status_changed" {
		switch strings.ToLower(request.Status) {
		case statusSolved, statusClosed:
			err = models.CloseTickets(ctx, s.DB, nil, []*models.Ticket{ticket}, false, l)
		case statusOpen:
			err = models.ReopenTickets(ctx, s.DB, nil, []*models.Ticket{ticket}, false, l)
		}

		if err != nil {
			return err, http.StatusBadRequest, nil
		}
	}

	return map[string]string{"status": "handled"}, http.StatusOK, nil
}
