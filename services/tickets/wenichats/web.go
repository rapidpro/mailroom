package wenichats

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/buger/jsonparser"
	"github.com/go-chi/chi"
	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/services/tickets"
	"github.com/nyaruka/mailroom/web"
	"github.com/pkg/errors"
)

func init() {
	base := "/mr/tickets/types/wenichats"
	web.RegisterJSONRoute(http.MethodPost, base+"/event_callback/{ticketer:[a-f0-9\\-]+}/{ticket:[a-f0-9\\-]+}", web.WithHTTPLogs(handleEventCallback))
}

type eventCallbackRequest struct {
	Type    string          `json:"type"`
	Content MessageResponse `json:"content"`
}

func handleEventCallback(ctx context.Context, rt *runtime.Runtime, r *http.Request, l *models.HTTPLogger) (interface{}, int, error) {
	ticketUUID := uuids.UUID(chi.URLParam(r, "ticket"))

	ticket, _, _, err := tickets.FromTicketUUID(ctx, rt, flows.TicketUUID(ticketUUID), typeWenichats)
	if err != nil {
		return errors.Errorf("no such ticket %s", ticketUUID), http.StatusNotFound, nil
	}

	oa, err := models.GetOrgAssets(ctx, rt, ticket.OrgID())
	if err != nil {
		return err, http.StatusBadRequest, nil
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		return err, http.StatusBadRequest, nil
	}

	eventType, err := jsonparser.GetString(body, "type")
	if err != nil {
		return err, http.StatusBadRequest, nil
	}

	switch eventType {
	case "msg.create":
		eMsg := &eventCallbackRequest{}
		if err := json.Unmarshal([]byte(body), eMsg); err != nil {
			return err, http.StatusInternalServerError, nil
		}

		if len(eMsg.Content.Media) > 0 {
			for _, m := range eMsg.Content.Media {
				file, err := tickets.FetchFile(m.URL, nil)
				if err != nil {
					return errors.Wrapf(err, "error fetching ticket file '%s'", m.URL), http.StatusInternalServerError, nil
				}
				file.ContentType = m.ContentType
				_, err = tickets.SendReply(ctx, rt, ticket, "", []*tickets.File{file})
				if err != nil {
					return errors.Wrapf(err, "error on send ticket reply with media '%s'", m.URL), http.StatusInternalServerError, nil
				}
			}
		}

		txtMsg := eMsg.Content.Text
		if strings.TrimSpace(txtMsg) != "" {
			_, err = tickets.SendReply(ctx, rt, ticket, txtMsg, nil)
			if err != nil {
				return errors.Wrapf(err, "error on send ticket reply"), http.StatusBadRequest, nil
			}
		}
	case "room.update":
		err = tickets.Close(ctx, rt, oa, ticket, false, nil)
		if err != nil {
			return errors.Wrapf(err, "error on close ticket"), http.StatusInternalServerError, nil
		}
	default:
		return errors.New("invalid event type"), http.StatusBadRequest, nil
	}

	return map[string]string{"status": "handled"}, http.StatusOK, nil
}
