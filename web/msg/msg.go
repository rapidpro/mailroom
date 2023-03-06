package msg

import (
	"context"
	"net/http"

	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/msgio"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/web"
	"github.com/pkg/errors"
)

func init() {
	web.RegisterRoute(http.MethodPost, "/mr/msg/send", web.RequireAuthToken(web.JSONPayload(handleSend)))
	web.RegisterRoute(http.MethodPost, "/mr/msg/resend", web.RequireAuthToken(web.JSONPayload(handleResend)))
}

// Request to send a message.
//
//	{
//	  "org_id": 1,
//	  "contact_id": 123456,
//	  "user_id": 56,
//	  "text": "hi there"
//	}
type sendRequest struct {
	OrgID       models.OrgID       `json:"org_id"       validate:"required"`
	UserID      models.UserID      `json:"user_id"      validate:"required"`
	ContactID   models.ContactID   `json:"contact_id"   validate:"required"`
	Text        string             `json:"text"`
	Attachments []utils.Attachment `json:"attachments"`
	TicketID    models.TicketID    `json:"ticket_id"`
}

// handles a request to resend the given messages
func handleSend(ctx context.Context, rt *runtime.Runtime, r *sendRequest) (any, int, error) {
	// grab our org
	oa, err := models.GetOrgAssets(ctx, rt, r.OrgID)
	if err != nil {
		return nil, 0, errors.Wrap(err, "unable to load org assets")
	}

	// load the contact and generate as a flow contact
	c, err := models.LoadContact(ctx, rt.DB, oa, r.ContactID)
	if err != nil {
		return nil, 0, errors.Wrap(err, "error loading contact")
	}

	contact, err := c.FlowContact(oa)
	if err != nil {
		return nil, 0, errors.Wrap(err, "error creating flow contact")
	}

	out, ch := models.NewMsgOut(oa, contact, r.Text, r.Attachments, nil, contact.Locale(oa.Env()))
	msg, err := models.NewOutgoingChatMsg(rt, oa.Org(), ch, contact, out, dates.Now(), r.UserID)
	if err != nil {
		return nil, 0, errors.Wrap(err, "error creating outgoing message")
	}

	err = models.InsertMessages(ctx, rt.DB, []*models.Msg{msg})
	if err != nil {
		return nil, 0, errors.Wrap(err, "error inserting outgoing message")
	}

	// if message was a ticket reply, update the ticket
	if r.TicketID != models.NilTicketID {
		if err := models.RecordTicketReply(ctx, rt.DB, oa, r.TicketID, r.UserID); err != nil {
			return nil, 0, errors.Wrap(err, "error recording ticket reply")
		}
	}

	msgio.SendMessages(ctx, rt, rt.DB, nil, []*models.Msg{msg})

	return map[string]any{
		"id":          msg.ID(),
		"channel":     out.Channel(),
		"contact":     contact.Reference(),
		"urn":         msg.URN(),
		"text":        msg.Text(),
		"attachments": msg.Attachments(),
		"status":      msg.Status(),
		"created_on":  msg.CreatedOn(),
		"modified_on": msg.ModifiedOn(),
	}, http.StatusOK, nil
}

// Request to resend failed messages.
//
//	{
//	  "org_id": 1,
//	  "msg_ids": [123456, 345678]
//	}
type resendRequest struct {
	OrgID  models.OrgID   `json:"org_id"   validate:"required"`
	MsgIDs []models.MsgID `json:"msg_ids"  validate:"required"`
}

// handles a request to resend the given messages
func handleResend(ctx context.Context, rt *runtime.Runtime, r *resendRequest) (any, int, error) {
	// grab our org
	oa, err := models.GetOrgAssets(ctx, rt, r.OrgID)
	if err != nil {
		return nil, 0, errors.Wrap(err, "unable to load org assets")
	}

	msgs, err := models.GetMessagesByID(ctx, rt.DB, r.OrgID, models.DirectionOut, r.MsgIDs)
	if err != nil {
		return nil, 0, errors.Wrap(err, "error loading messages to resend")
	}

	resends, err := models.ResendMessages(ctx, rt.DB, rt.RP, oa, msgs)
	if err != nil {
		return nil, 0, errors.Wrap(err, "error resending messages")
	}

	msgio.SendMessages(ctx, rt, rt.DB, nil, resends)

	// response is the ids of the messages that were actually resent
	resentMsgIDs := make([]flows.MsgID, len(resends))
	for i, m := range resends {
		resentMsgIDs[i] = m.ID()
	}
	return map[string]any{"msg_ids": resentMsgIDs}, http.StatusOK, nil
}
