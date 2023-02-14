package msgs

import (
	"context"
	"time"

	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/pkg/errors"
)

const TypeSendMsg = "send_msg" // TypeSendMsg is the task type for sending a single message

func init() {
	tasks.RegisterType(TypeSendMsg, func() tasks.Task { return &SendMsgTask{} })
}

// SendMsgTask is the task to send a message
type SendMsgTask struct {
	ContactID   models.ContactID   `json:"contact_id"`
	Text        string             `json:"text"`
	Attachments []utils.Attachment `json:"attachments"`
	UserID      models.UserID      `json:"user_id"`
	TicketID    models.TicketID    `json:"ticket_id"`
}

func (t *SendMsgTask) Type() string {
	return TypeSendMsg
}

// Timeout is the maximum amount of time the task can run for
func (t *SendMsgTask) Timeout() time.Duration {
	return time.Minute * 60
}

func (t *SendMsgTask) Perform(ctx context.Context, rt *runtime.Runtime, orgID models.OrgID) error {
	oa, err := models.GetOrgAssets(ctx, rt, orgID)
	if err != nil {
		return errors.Wrap(err, "error getting org assets")
	}

	// load all our contacts
	c, err := models.LoadContact(ctx, rt.DB, oa, t.ContactID)
	if err != nil {
		return errors.Wrap(err, "error loading contact")
	}

	contact, err := c.FlowContact(oa)
	if err != nil {
		return errors.Wrap(err, "error creating flow contact")
	}

	out, ch := models.NewMsgOut(oa, contact, t.Text, t.Attachments, nil, contact.Locale(oa.Env()))
	msg, err := models.NewOutgoingChatMsg(rt, oa.Org(), ch, contact, out, time.Now())
	if err != nil {
		return errors.Wrap(err, "error creating outgoing message")
	}

	err = models.InsertMessages(ctx, rt.DB, []*models.Msg{msg})
	if err != nil {
		return errors.Wrap(err, "error inserting outgoing message")
	}

	// if message was a ticket reply, update the ticket
	if t.TicketID != models.NilTicketID {
		if err := models.RecordTicketReply(ctx, rt.DB, oa, t.TicketID, t.UserID); err != nil {
			return errors.Wrap(err, "error recording ticket reply")
		}
	}

	return nil
}
