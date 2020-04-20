package mailgun

import (
	"context"
	"net/http"
	"regexp"

	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/goflow/utils/uuids"
	"github.com/nyaruka/mailroom/courier"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/web"
	"github.com/nyaruka/null"

	"github.com/pkg/errors"
)

func init() {
	web.RegisterJSONRoute(http.MethodPost, "/mr/tickets/mailgun/receive", web.RequireAuthToken(handleReceive))
}

type receiveRequest struct {
	Recipient    string `form:"recipient"     validate:"email"`
	ReplyTo      string `form:"Reply-To"`
	From         string `form:"From"          validate:"required"` // TODO: should be validated against org config?
	MessageID    string `form:"Message-Id"    validate:"required"`
	Subject      string `form:"subject"       validate:"required"`
	PlainBody    string `form:"body-plain"    validate:"required"`
	StrippedText string `form:"stripped-text" validate:"required"`
	HTMLBody     string `form:"body-html"`
	Token        string `form:"token"         validate:"required"` // TODO: should be validated
	Signature    string `form:"signature"     validate:"required"` // TODO: should be validated
}

type receiveResponse struct {
	Action     string      `json:"action"`
	TicketUUID string      `json:"ticket_uuid"`
	ExternalID null.String `json:"external_id"`
	Message    string      `json:"message"`
	Status     string      `json:"status"`
}

var emailRegex = regexp.MustCompile(`^.*<(.*?)>$`)
var addressRegex = regexp.MustCompile(`^ticket\+([0-9a-fA-F]{8}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{12})@.*$`)

func handleReceive(ctx context.Context, s *web.Server, r *http.Request) (interface{}, int, error) {
	request := &receiveRequest{}
	if err := web.DecodeAndValidateForm(request, r); err != nil {
		return errors.Wrapf(err, "error decoding form"), http.StatusBadRequest, nil
	}

	// recipient is in the format t+<ticket-uuid>@... parse it out
	match := addressRegex.FindAllStringSubmatch(request.Recipient, -1)
	if len(match) != 1 || len(match[0]) != 2 {
		return errors.Errorf("invalid recipient, ignoring: %s", request.Recipient), http.StatusOK, nil
	}
	ticketUUID := match[0][1]

	// parse the reply to
	replyTo := request.ReplyTo

	// no reply-to header? just use from
	if replyTo == "" {
		replyTo = request.From
	}

	// reply-to and from are in the format `Foo Bar <foo@bar.com>` parse just the address out
	match = emailRegex.FindAllStringSubmatch(replyTo, -1)
	if len(match) == 1 && len(match[0]) == 2 {
		replyTo = match[0][1]
	}
	if replyTo == "" {
		return errors.New("missing reply-to or from"), http.StatusBadRequest, nil
	}

	// look up our ticket
	ticket, err := models.LookupTicketByUUID(ctx, s.DB, uuids.UUID(ticketUUID))
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "error looking up ticket: %s", ticketUUID)
	}
	if ticket == nil {
		return errors.Errorf("invalid ticket uuid, ignoring"), http.StatusOK, nil
	}

	// update our thread
	config := null.NewMap(map[string]interface{}{
		"last-message-id": request.MessageID,
		"subject":         request.Subject,
		"reply-to":        replyTo,
	})
	err = models.UpdateTicket(ctx, s.DB, ticket, null.String(replyTo), "O", config)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "error updating ticket: %s", ticket.UUID())
	}

	// TODO: below message creation stuff should be moved into models

	// look up our assets
	assets, err := models.GetOrgAssets(s.CTX, s.DB, ticket.OrgID())
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "error looking up org: %d", ticket.OrgID())
	}

	// we build a simple translation
	translations := map[envs.Language]*models.BroadcastTranslation{
		envs.Language(""): &models.BroadcastTranslation{Text: request.StrippedText},
	}

	// we'll use a broadcast to send this message
	bcast := models.NewBroadcast(assets.OrgID(), models.NilBroadcastID, translations, models.TemplateStateEvaluated, envs.Language(""), nil, nil, nil)
	batch := bcast.CreateBatch([]models.ContactID{ticket.ContactID()})
	msgs, err := models.CreateBroadcastMessages(s.CTX, s.DB, s.RP, assets, batch)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "error creating message batch")
	}

	// queue our message
	rc := s.RP.Get()
	defer rc.Close()

	err = courier.QueueMessages(rc, msgs)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "error queuing outgoing message")
	}

	return &receiveResponse{
		Action:     "forwarded",
		TicketUUID: string(ticket.UUID()),
		ExternalID: ticket.ExternalID(),
		Status:     ticket.Status(),
		Message:    request.StrippedText,
	}, http.StatusOK, nil
}
