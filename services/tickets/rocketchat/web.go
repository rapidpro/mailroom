package rocketchat

import (
	"context"
	"encoding/json"
	"github.com/go-chi/chi"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/services/tickets"
	"github.com/nyaruka/mailroom/web"
	"github.com/pkg/errors"
	"net/http"
)

func init() {
	base := "/mr/tickets/types/rocketchat"

	web.RegisterJSONRoute(http.MethodPost, base+"/event_callback", web.WithHTTPLogs(handleEventCallback))
}

type eventCallbackRequest struct {
	Type     string          `json:"type"     validate:"required"`
	TicketID string          `json:"ticketId" validate:"required"`
	Data     json.RawMessage `json:"data"`
}

type agentMessage struct {
	Text        string   `json:"text"`
	Attachments []string `json:"attachments"`
}

func handleEventCallback(ctx context.Context, s *web.Server, r *http.Request, l *models.HTTPLogger) (interface{}, int, error) {
	ticketerUUID := assets.TicketerUUID(chi.URLParam(r, "ticketer"))

	// look up ticketer
	ticketer, _, err := tickets.FromTicketerUUID(ctx, s.DB, ticketerUUID, typeRocketChat)
	if err != nil {
		return errors.Errorf("no such ticketer %s", ticketerUUID), http.StatusNotFound, nil
	}

	// check secret
	secret := r.Header.Get("Authorization")
	if ticketer.Config(configSecret) != secret {
		return map[string]string{"status": "unauthorized"}, http.StatusUnauthorized, nil
	}

	request := &eventCallbackRequest{}
	if err := utils.UnmarshalAndValidateWithLimit(r.Body, request, web.MaxRequestBytes); err != nil {
		return err, http.StatusBadRequest, nil
	}

	// look up ticket
	ticket, _, _, err := tickets.FromTicketUUID(ctx, s.DB, flows.TicketUUID(request.TicketID), typeRocketChat)
	if err != nil {
		return errors.Errorf("no such ticket %s", request.TicketID), http.StatusNotFound, nil
	}

	// handle event callback
	switch request.Type {

	case "agent-message":
		data := &agentMessage{}
		if err := utils.UnmarshalAndValidate(request.Data, data); err != nil {
			return err, http.StatusBadRequest, nil
		}

		_, err = tickets.SendReply(ctx, s.DB, s.RP, ticket, data.Text)

	case "close-room":
		err = models.CloseTickets(ctx, s.DB, nil, []*models.Ticket{ticket}, false, l)

	}
	if err != nil {
		return err, http.StatusBadRequest, nil
	}

	return map[string]string{"status": "handled"}, http.StatusOK, nil
}
