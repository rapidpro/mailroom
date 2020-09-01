package mailgun

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/services/tickets"
	"github.com/nyaruka/mailroom/web"

	"github.com/pkg/errors"
)

func init() {
	base := "/mr/tickets/types/mailgun"

	web.RegisterJSONRoute(http.MethodPost, base+"/receive", web.WithHTTPLogs(handleReceive))
}

type receiveRequest struct {
	Recipient    string `form:"recipient"     validate:"required,email"`
	Sender       string `form:"sender"        validate:"required,email"`
	From         string `form:"From"`
	ReplyTo      string `form:"Reply-To"`
	MessageID    string `form:"Message-Id"    validate:"required"`
	Subject      string `form:"subject"       validate:"required"`
	PlainBody    string `form:"body-plain"`
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

// what we send back to mailgun.. this is mostly for our own since logging since they don't parse this
type receiveResponse struct {
	Action     string           `json:"action"`
	TicketUUID flows.TicketUUID `json:"ticket_uuid"`
	MsgUUID    flows.MsgUUID    `json:"msg_uuid,omitempty"`
}

var addressRegex = regexp.MustCompile(`^ticket\+([0-9a-fA-F]{8}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{12})@.*$`)

func handleReceive(ctx context.Context, s *web.Server, r *http.Request, l *models.HTTPLogger) (interface{}, int, error) {
	request := &receiveRequest{}
	if err := web.DecodeAndValidateForm(request, r); err != nil {
		return errors.Wrapf(err, "error decoding form"), http.StatusBadRequest, nil
	}

	if !request.verify(s.Config.MailgunSigningKey) {
		return errors.New("request signature validation failed"), http.StatusForbidden, nil
	}

	// recipient is in the format ticket+<ticket-uuid>@... parse it out
	match := addressRegex.FindAllStringSubmatch(request.Recipient, -1)
	if len(match) != 1 || len(match[0]) != 2 {
		return errors.Errorf("invalid recipient: %s", request.Recipient), http.StatusBadRequest, nil
	}

	// look up the ticket and ticketer
	ticket, ticketer, svc, err := tickets.FromTicketUUID(s.CTX, s.DB, flows.TicketUUID(match[0][1]), typeMailgun)
	if err != nil {
		return err, http.StatusBadRequest, nil
	}
	mailgun := svc.(*service)

	// check that this sender is allowed to send to this ticket
	configuredAddress := ticketer.Config(configToAddress)
	if request.Sender != configuredAddress {
		body := fmt.Sprintf("The address %s is not allowed to reply to this ticket\n", request.Sender)

		mailgun.send(mailgun.noReplyAddress(), request.From, "Ticket reply rejected", body, nil, nil, l.Ticketer(ticketer))

		return &receiveResponse{Action: "rejected", TicketUUID: ticket.UUID()}, http.StatusOK, nil
	}

	// check if reply is actually a command
	if strings.ToLower(strings.TrimSpace(request.StrippedText)) == "close" {
		org, _ := models.GetOrgAssets(ctx, s.DB, ticket.OrgID())
		err = models.CloseTickets(ctx, s.DB, org, []*models.Ticket{ticket}, true, l)
		if err != nil {
			return errors.Wrapf(err, "error closing ticket: %s", ticket.UUID()), http.StatusInternalServerError, nil
		}

		return &receiveResponse{Action: "closed", TicketUUID: ticket.UUID()}, http.StatusOK, nil
	}

	// update our ticket
	config := map[string]string{
		ticketConfigLastMessageID: request.MessageID,
	}
	err = models.UpdateAndKeepOpenTicket(ctx, s.DB, ticket, config)
	if err != nil {
		return errors.Wrapf(err, "error updating ticket: %s", ticket.UUID()), http.StatusInternalServerError, nil
	}

	msg, err := tickets.SendReply(ctx, s.DB, s.RP, s.Storage, ticket, request.StrippedText, nil)
	if err != nil {
		return err, http.StatusInternalServerError, nil
	}

	return &receiveResponse{Action: "forwarded", TicketUUID: ticket.UUID(), MsgUUID: msg.UUID()}, http.StatusOK, nil
}
