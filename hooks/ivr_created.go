package hooks

import (
	"context"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/models"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func init() {
	models.RegisterEventHook(events.TypeIVRCreated, handleIVRCreated)
}

// CommitIVRHook is our hook for comitting session messages / say commands
type CommitIVRHook struct{}

var commitIVRHook = &CommitIVRHook{}

// Apply takes care of inserting all the messages in the passed in sessions assigning topups to them as needed.
func (h *CommitIVRHook) Apply(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, sessions map[*models.Session][]interface{}) error {
	msgs := make([]*models.Msg, 0, len(sessions))
	for _, s := range sessions {
		for _, m := range s {
			msgs = append(msgs, m.(*models.Msg))
		}
	}

	// find the topup we will assign
	rc := rp.Get()
	topup, err := models.DecrementOrgCredits(ctx, tx, rc, org.OrgID(), len(msgs))
	rc.Close()
	if err != nil {
		return errors.Wrapf(err, "error finding active topup")
	}

	// if we have an active topup, assign it to our messages
	if topup != models.NilTopupID {
		for _, m := range msgs {
			m.SetTopup(topup)
		}
	}

	// insert all our messages
	err = models.InsertMessages(ctx, tx, msgs)
	if err != nil {
		return errors.Wrapf(err, "error writing messages")
	}

	return nil
}

// handleIVRCreated creates the db msg for the passed in event
func handleIVRCreated(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, session *models.Session, e flows.Event) error {
	event := e.(*events.IVRCreatedEvent)

	logrus.WithFields(logrus.Fields{
		"contact_uuid": session.ContactUUID(),
		"session_id":   session.ID,
		"text":         event.Msg.Text(),
	}).Debug("ivr say")

	// get our channel connection
	conn := session.ChannelSession()
	if conn == nil {
		return errors.Errorf("ivr sessions must have a channel session set")
	}

	msg, err := models.NewOutgoingIVR(org.OrgID(), conn, event.Msg, event.CreatedOn())
	if err != nil {
		return errors.Wrapf(err, "error creating outgoing ivr say: %s", event.Msg.Text())
	}

	// register to have this message committed
	session.AddPreCommitEvent(commitIVRHook, msg)

	return nil
}
