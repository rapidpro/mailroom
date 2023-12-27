package wenichats

import (
	"bytes"
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

var (
	mb5   = 5 * 1024 * 1024
	mb16  = 16 * 1024 * 1024
	mb100 = 100 * 1024 * 1024
)

var mediaTypeMaxBodyBytes = map[string]int{
	"text/plain":                    mb100,
	"application/pdf":               mb100,
	"application/vnd.ms-powerpoint": mb100,
	"application/msword":            mb100,
	"application/vnd.ms-excel":      mb100,
	"application/vnd.openxmlformats-officedocument.wordprocessingml.document":   mb100,
	"application/vnd.openxmlformats-officedocument.presentationml.presentation": mb100,
	"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":         mb100,
	"audio/aac": mb16, "audio/mp4": mb16, "audio/mpeg": mb16, "audio/amr": mb16,
	"image/jpeg": mb5, "image/png": mb5,
	"video/mp4": mb16, "video/3gp": mb16,
}

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
				file, err := tickets.FetchFileWithMaxSize(m.URL, nil, 100*1024*1024)
				if err != nil {
					return errors.Wrapf(err, "error fetching ticket file '%s'", m.URL), http.StatusInternalServerError, nil
				}
				file.ContentType = m.ContentType

				maxBodyBytes := mediaTypeMaxBodyBytes[file.ContentType]
				if maxBodyBytes == 0 {
					maxBodyBytes = mb100
				}
				bodyReader := io.LimitReader(file.Body, int64(maxBodyBytes)+1)
				bodyBytes, err := io.ReadAll(bodyReader)
				if err != nil {
					return err, http.StatusBadRequest, nil
				}
				if bodyReader.(*io.LimitedReader).N <= 0 {
					return errors.Wrapf(err, "unable to send media type %s because response body exceeds %d bytes limit", file.ContentType, maxBodyBytes), http.StatusBadRequest, nil
				}
				file.Body = io.NopCloser(bytes.NewReader(bodyBytes))
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
