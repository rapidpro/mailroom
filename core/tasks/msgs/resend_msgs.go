package msgs

import (
	"context"
	"time"

	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/msgio"
	"github.com/nyaruka/mailroom/core/tasks"

	"github.com/pkg/errors"
)

// TypeResendMsgs is the type of the resend messages task
const TypeResendMsgs = "resend_msgs"

func init() {
	tasks.RegisterType(TypeResendMsgs, func() tasks.Task { return &ResendMsgsTask{} })
}

// ResendMsgsTask is our task for resending messages
type ResendMsgsTask struct {
	MsgIDs []models.MsgID `json:"msg_ids"`
}

// Timeout is the maximum amount of time the task can run for
func (t *ResendMsgsTask) Timeout() time.Duration {
	return time.Minute * 5
}

func (t *ResendMsgsTask) Perform(ctx context.Context, mr *mailroom.Mailroom, orgID models.OrgID) error {
	db := mr.DB
	rp := mr.RP

	oa, err := models.GetOrgAssets(ctx, mr.DB, orgID)
	if err != nil {
		return errors.Wrap(err, "unable to load org")
	}

	msgs, err := models.LoadMessages(ctx, db, orgID, models.DirectionOut, t.MsgIDs)
	if err != nil {
		return errors.Wrap(err, "error loading messages to resend")
	}

	clones, err := models.CloneMessages(ctx, db, rp, oa, msgs)
	if err != nil {
		return errors.Wrap(err, "error cloning messages")
	}

	// update existing messages as RESENT
	err = models.UpdateMessageStatus(ctx, db, msgs, models.MsgStatusResent)
	if err != nil {
		return errors.Wrap(err, "error updating message status")
	}

	msgio.SendMessages(ctx, db, rp, nil, clones)
	return nil
}
