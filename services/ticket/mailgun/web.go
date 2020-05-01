package mailgun

import (
	"context"
	"net/http"
	"regexp"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/courier"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/web"
	"github.com/nyaruka/null"

	"github.com/pkg/errors"
)

func init() {
	web.RegisterJSONRoute(http.MethodPost, "/mr/ticket/mailgun/receive", web.RequireAuthToken(handleReceive))
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
	Action     string              `json:"action"`
	TicketUUID string              `json:"ticket_uuid"`
	ExternalID null.String         `json:"external_id"`
	Message    string              `json:"message"`
	Status     models.TicketStatus `json:"status"`
}

var addressRegex = regexp.MustCompile(`^ticket\+([0-9a-fA-F]{8}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{12})@.*$`)

func handleReceive(ctx context.Context, s *web.Server, r *http.Request) (interface{}, int, error) {
	request := &receiveRequest{}
	if err := web.DecodeAndValidateForm(request, r); err != nil {
		return errors.Wrapf(err, "error decoding form"), http.StatusBadRequest, nil
	}

	// recipient is in the format ticket+<ticket-uuid>@... parse it out
	match := addressRegex.FindAllStringSubmatch(request.Recipient, -1)
	if len(match) != 1 || len(match[0]) != 2 {
		return errors.Errorf("invalid recipient, ignoring: %s", request.Recipient), http.StatusOK, nil
	}
	ticketUUID := flows.TicketUUID(match[0][1])

	// look up our ticket
	ticket, err := models.LookupTicketByUUID(ctx, s.DB, ticketUUID)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "error looking up ticket: %s", ticketUUID)
	}
	if ticket == nil {
		return errors.Errorf("invalid ticket uuid, ignoring"), http.StatusOK, nil
	}

	// update our ticket
	config := map[string]string{
		"last-message-id": request.MessageID,
		"last-subject":    request.Subject,
	}
	err = models.UpdateTicket(ctx, s.DB, ticket, models.TicketStatusOpen, config)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "error updating ticket: %s", ticket.UUID())
	}

	msg, err := ticket.CreateReply(s.CTX, s.DB, s.RP, request.StrippedText)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "error creating ticket reply")
	}

	// queue our message
	rc := s.RP.Get()
	defer rc.Close()

	err = courier.QueueMessages(rc, []*models.Msg{msg})
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
