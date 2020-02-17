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
	models.RegisterEventHandler(events.TypeIVRCreated, handleIVRCreated)
}

// CommitIVRHook is our hook for comitting scene messages / say commands
type CommitIVRHook struct{}

var commitIVRHook = &CommitIVRHook{}

// Apply takes care of inserting all the messages in the passed in scene assigning topups to them as needed.
func (h *CommitIVRHook) Apply(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, scenes map[*models.Scene][]interface{}) error {
	msgs := make([]*models.Msg, 0, len(scenes))
	for _, s := range scenes {
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
func handleIVRCreated(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, scene *models.Scene, e flows.Event) error {
	event := e.(*events.IVRCreatedEvent)
	logrus.WithFields(logrus.Fields{
		"contact_uuid": scene.ContactUUID(),
		"session_id":   scene.SessionID(),
		"text":         event.Msg.Text(),
	}).Debug("ivr say")

	// get our channel connection
	conn := scene.Session().ChannelConnection()
	if conn == nil {
		return errors.Errorf("ivr session must have a channel connection set")
	}

	// if our call is no longer in progress, return
	if conn.Status() != models.ConnectionStatusInProgress {
		return nil
	}

	msg, err := models.NewOutgoingIVR(org.OrgID(), conn, event.Msg, event.CreatedOn())
	if err != nil {
		return errors.Wrapf(err, "error creating outgoing ivr say: %s", event.Msg.Text())
	}

	// register to have this message committed
	scene.AppendToEventPreCommitHook(commitIVRHook, msg)

	return nil
}
