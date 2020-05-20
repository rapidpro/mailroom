package mailgun

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"regexp"
	"strings"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/courier"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/web"

	"github.com/pkg/errors"
)

func init() {
	web.RegisterJSONRoute(http.MethodPost, "/mr/ticket/mailgun/receive", handleReceive)
}

type receiveRequest struct {
	Recipient    string `form:"recipient"     validate:"email"`
	ReplyTo      string `form:"Reply-To"`
	From         string `form:"From"          validate:"required"`
	MessageID    string `form:"Message-Id"    validate:"required"`
	Subject      string `form:"subject"       validate:"required"`
	PlainBody    string `form:"body-plain"    validate:"required"`
	StrippedText string `form:"stripped-text" validate:"required"`
	HTMLBody     string `form:"body-html"`
	Timestamp    string `form:"timestamp"     validate:"required"`
	Token        string `form:"token"         validate:"required"`
	Signature    string `form:"signature"     validate:"required"`
}

// see https://documentation.mailgun.com/en/latest/user_manual.html#securing-webhooks
func (r *receiveRequest) verify(signingKey string) bool {
	v := r.Timestamp + r.Token

	mac := hmac.New(sha256.New, []byte(signingKey))
	mac.Write([]byte(v))
	expectedMAC := hex.EncodeToString(mac.Sum(nil))

	return hmac.Equal([]byte(r.Signature), []byte(expectedMAC))
}

type receiveResponse struct {
	Action     string `json:"action"`
	TicketUUID string `json:"ticket_uuid"`
}

var addressRegex = regexp.MustCompile(`^ticket\+([0-9a-fA-F]{8}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{12})@.*$`)

func handleReceive(ctx context.Context, s *web.Server, r *http.Request) (interface{}, int, error) {
	request := &receiveRequest{}
	if err := web.DecodeAndValidateForm(request, r); err != nil {
		return errors.Wrapf(err, "error decoding form"), http.StatusBadRequest, nil
	}

	if !request.verify(s.Config.MailgunSigningKey) {
		return errors.New("request signature validation failed"), http.StatusBadRequest, nil
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

	// look up our assets and get the ticketer for this ticket
	assets, err := models.GetOrgAssets(s.CTX, s.DB, ticket.OrgID())
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "error looking up org: %d", ticket.OrgID())
	}
	ticketer := assets.TicketerByID(ticket.TicketerID())
	if ticketer == nil {
		return nil, http.StatusInternalServerError, errors.Errorf("error looking up ticketer: %d", ticket.TicketerID())
	}

	configuredAddress := ticketer.Config(configToAddress)
	if request.From != configuredAddress {
		// TODO reply back to the sender to explain this

		return nil, http.StatusInternalServerError, errors.Errorf("address %s not permitted to reply to this ticket", request.From)
	}

	// check if reply is actually a command
	if strings.ToLower(strings.TrimSpace(request.StrippedText)) == "close" {
		err = models.CloseTickets(ctx, s.DB, assets, []*models.Ticket{ticket})
		if err != nil {
			return nil, http.StatusInternalServerError, errors.Wrapf(err, "error closing ticket: %s", ticket.UUID())
		}

		return &receiveResponse{Action: "closed", TicketUUID: string(ticket.UUID())}, http.StatusOK, nil
	}

	// update our ticket
	config := map[string]string{
		"last-message-id": request.MessageID,
		"last-subject":    request.Subject,
	}
	err = models.UpdateAndKeepOpenTicket(ctx, s.DB, ticket, config)
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

	return &receiveResponse{Action: "forwarded", TicketUUID: string(ticket.UUID())}, http.StatusOK, nil
}
