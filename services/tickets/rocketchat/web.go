package rocketchat

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/go-chi/chi"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/services/tickets"
	"github.com/nyaruka/mailroom/web"
	"github.com/pkg/errors"
)

func init() {
	base := "/mr/tickets/types/rocketchat"

	web.RegisterJSONRoute(http.MethodPost, base+"/event_callback/{ticketer:[a-f0-9\\-]+}", web.WithHTTPLogs(handleEventCallback))
}

type eventCallbackRequest struct {
	Type     string          `json:"type"     validate:"required"`
	TicketID string          `json:"ticketID" validate:"required"`
	Data     json.RawMessage `json:"data"`
}

type agentMessageData struct {
	Text        string `json:"text"`
	Attachments []struct {
		Type string `json:"type"`
		URL  string `json:"url"`
	} `json:"attachments"`
}

func handleEventCallback(ctx context.Context, rt *runtime.Runtime, r *http.Request, l *models.HTTPLogger) (interface{}, int, error) {
	ticketerUUID := assets.TicketerUUID(chi.URLParam(r, "ticketer"))

	// look up ticketer
	ticketer, _, err := tickets.FromTicketerUUID(ctx, rt, ticketerUUID, typeRocketChat)
	if err != nil {
		return errors.Errorf("no such ticketer %s", ticketerUUID), http.StatusNotFound, nil
	}

	// check secret
	secret := r.Header.Get("Authorization")
	if fmt.Sprintf("Token %s", ticketer.Config(configSecret)) != secret {
		return map[string]string{"status": "unauthorized"}, http.StatusUnauthorized, nil
	}

	request := &eventCallbackRequest{}
	if err := utils.UnmarshalAndValidateWithLimit(r.Body, request, web.MaxRequestBytes); err != nil {
		return err, http.StatusBadRequest, nil
	}

	// look up ticket
	ticket, _, _, err := tickets.FromTicketUUID(ctx, rt, flows.TicketUUID(request.TicketID), typeRocketChat)
	if err != nil {
		return errors.Errorf("no such ticket %s", request.TicketID), http.StatusNotFound, nil
	}

	oa, err := models.GetOrgAssets(ctx, rt, ticket.OrgID())
	if err != nil {
		return err, http.StatusBadRequest, nil
	}

	// handle event callback
	switch request.Type {

	case "agent-message":
		data := &agentMessageData{}
		if err := utils.UnmarshalAndValidate(request.Data, data); err != nil {
			return err, http.StatusBadRequest, nil
		}

		// fetch files
		files := make([]*tickets.File, len(data.Attachments))
		for i, attachment := range data.Attachments {
			headers := map[string]string{
				"X-Auth-Token": ticketer.Config(configAdminAuthToken),
				"X-User-Id":    ticketer.Config(configAdminUserID),
			}
			files[i], err = tickets.FetchFile(attachment.URL, headers)
			if err != nil {
				return errors.Wrapf(err, "error fetching ticket file '%s'", attachment), http.StatusBadRequest, nil
			}
		}

		var attachments []string
		for _, attachment := range data.Attachments {
			attachments = append(attachments, attachment.URL)
		}

		_, err = tickets.SendReply(ctx, rt, ticket, data.Text, files)

	case "close-room":
		err = tickets.Close(ctx, rt, oa, ticket, false, l)

	default:
		err = errors.New("invalid event type")

	}
	if err != nil {
		return err, http.StatusBadRequest, nil
	}

	return map[string]string{"status": "handled"}, http.StatusOK, nil
}
