package twilioflex

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/go-chi/chi"
	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/services/tickets"
	"github.com/nyaruka/mailroom/web"
	"github.com/pkg/errors"
)

func init() {
	base := "/mr/tickets/types/twilioflex"
	web.RegisterJSONRoute(http.MethodPost, base+"/event_callback/{ticketer:[a-f0-9\\-]+}/{ticket:[a-f0-9\\-]+}", web.WithHTTPLogs(handleEventCallback))
}

type eventCallbackRequest struct {
	EventType        string     `json:"event_type,omitempty"`
	InstanceSid      string     `json:"instance_sid,omitempty"`
	Attributes       string     `json:"attributes,omitempty"`
	DateCreated      *time.Time `json:"date_created,omitempty"`
	Index            int        `json:"index,omitempty"`
	From             string     `json:"from,omitempty"`
	MessageSid       string     `json:"message_sid,omitempty"`
	AccountSid       string     `json:"account_sid,omitempty"`
	Source           string     `json:"source,omitempty"`
	ChannelSid       string     `json:"channel_sid,omitempty"`
	ClientIdentity   string     `json:"client_identity,omitempty"`
	RetryCount       int        `json:"retry_count,omitempty"`
	WebhookType      string     `json:"webhook_type,omitempty"`
	Body             string     `json:"body,omitempty"`
	WebhookSid       string     `json:"webhook_sid,omitempty"`
	MediaSid         string     `json:"media_sid,omitempty"`
	MediaSize        string     `json:"media_size,omitempty"`
	MediaContentType string     `json:"media_content_type,omitempty"`
	MediaFilename    string     `json:"media_filename,omitempty"`
}

func handleEventCallback(ctx context.Context, rt *runtime.Runtime, r *http.Request, l *models.HTTPLogger) (interface{}, int, error) {
	ticketerUUID := assets.TicketerUUID(chi.URLParam(r, "ticketer"))
	request := &eventCallbackRequest{}
	if err := web.DecodeAndValidateForm(request, r); err != nil {
		return errors.Wrapf(err, "error decoding form"), http.StatusBadRequest, nil
	}

	ticketer, _, err := tickets.FromTicketerUUID(ctx, rt, ticketerUUID, typeTwilioFlex)
	if err != nil {
		return errors.Errorf("no such ticketer %s", ticketerUUID), http.StatusNotFound, nil
	}

	accountSid := request.AccountSid
	if accountSid != ticketer.Config(configurationAccountSid) {
		return map[string]string{"status": "unauthorized"}, http.StatusUnauthorized, nil
	}

	ticketUUID := uuids.UUID(chi.URLParam(r, "ticket"))

	ticket, _, _, err := tickets.FromTicketUUID(ctx, rt, flows.TicketUUID(ticketUUID), typeTwilioFlex)
	if err != nil {
		return errors.Errorf("no such ticket %s", ticketUUID), http.StatusNotFound, nil
	}

	oa, err := models.GetOrgAssets(ctx, rt, ticket.OrgID())
	if err != nil {
		return err, http.StatusBadRequest, nil
	}

	switch request.EventType {
	case "onMessageSent":
		_, err = tickets.SendReply(ctx, rt, ticket, request.Body, []*tickets.File{})
		if err != nil {
			return err, http.StatusBadRequest, nil
		}
	case "onMediaMessageSent":
		config := ticketer.Config
		authToken := config(configurationAuthToken)
		accountSid := config(configurationAccountSid)
		chatServiceSid := config(configurationChatServiceSid)
		workspaceSid := config(configurationWorkspaceSid)
		flexFlowSid := config(configurationFlexFlowSid)

		client := NewClient(http.DefaultClient, nil, authToken, accountSid, chatServiceSid, workspaceSid, flexFlowSid)

		mediaContent, _, err := client.FetchMedia(request.MediaSid)
		if err != nil {
			return err, http.StatusBadRequest, nil
		}
		file, err := tickets.FetchFile(mediaContent.Links.ContentDirectTemporary, nil)
		file.ContentType = mediaContent.ContentType
		if err != nil {
			return errors.Wrapf(err, "error fetching ticket file '%s'", mediaContent.Links.ContentDirectTemporary), http.StatusBadRequest, nil
		}
		_, err = tickets.SendReply(ctx, rt, ticket, request.Body, []*tickets.File{file})
		if err != nil {
			return err, http.StatusBadRequest, nil
		}
	case "onChannelUpdated":
		jsonMap := make(map[string]interface{})
		err = json.Unmarshal([]byte(request.Attributes), &jsonMap)
		if err != nil {
			return err, http.StatusBadRequest, nil
		}
		if jsonMap["status"] == "INACTIVE" {
			err = tickets.Close(ctx, rt, oa, ticket, false, nil)
			if err != nil {
				return err, http.StatusBadRequest, nil
			}
		}
	}
	return map[string]string{"status": "handled"}, http.StatusOK, nil
}
