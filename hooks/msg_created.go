package hooks

import (
	"context"

	"github.com/nyaruka/gocommon/urns"

	"github.com/apex/log"
	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/courier"
	"github.com/nyaruka/mailroom/models"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func init() {
	models.RegisterEventHook(events.TypeMsgCreated, ApplyMsgCreatedEvent)
}

// SendSessionMessages is our hook for sending session messages
type SendSessionMessages struct{}

var sendSessionMessages = &SendSessionMessages{}

// Apply sends all non-android messages to courier
func (h *SendSessionMessages) Apply(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, sessions map[*models.Session][]interface{}) error {
	rc := rp.Get()
	defer rc.Close()

	for s, args := range sessions {
		// create our message array
		msgs := make([]*models.Msg, len(args))
		for i, m := range args {
			msgs[i] = m.(*models.Msg)
		}

		log := log.WithField("messages", msgs).WithField("session", s.ID)

		err := courier.QueueMessages(rc, msgs)

		// not being able to queue a message isn't the end of the world, log but don't return an error
		if err != nil {
			log.WithError(err).Error("error queuing message")

			// in the case of errors we do want to change the messages back to pending however so they
			// get queued later. (for the common case messages are only inserted and queued, without a status update)
			err = models.MarkMessagesPending(ctx, tx, msgs)
			if err != nil {
				log.WithError(err).Error("error marking message as pending")
			}
		}
	}

	return nil
}

// CommitSessionMessages is our hook for comitting session messages
type CommitSessionMessages struct{}

var commitSessionMessages = &CommitSessionMessages{}

// Apply takes care of inserting all the messages in the passed in sessions assigning topups to them as needed.
func (h *CommitSessionMessages) Apply(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, sessions map[*models.Session][]interface{}) error {
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

// ApplyMsgCreatedEvent creates the db msg for the passed in event
func ApplyMsgCreatedEvent(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, session *models.Session, e flows.Event) error {
	event := e.(*events.MsgCreatedEvent)

	// ignore events that don't have a channel or URN set
	// TODO: maybe we should create these messages in a failed state?
	if event.Msg.URN() == urns.NilURN || event.Msg.Channel() == nil {
		return nil
	}

	logrus.WithFields(logrus.Fields{
		"contact_uuid": session.ContactUUID(),
		"session_id":   session.ID,
		"text":         event.Msg.Text(),
		"urn":          event.Msg.URN(),
	}).Debug("creating message")

	// get our channel
	channel := org.ChannelByUUID(event.Msg.Channel().UUID)
	if channel == nil {
		return errors.Errorf("unable to load channel with uuid: %s", event.Msg.Channel().UUID)
	}

	msg, err := models.NewOutgoingMsg(org.OrgID(), channel, session.ContactID, &event.Msg, event.CreatedOn())
	if err != nil {
		return errors.Wrapf(err, "error creating outgoing message to %s", event.Msg.URN())
	}

	// set our reply to as well (will be noop in cases whren there is no incoming message)
	msg.SetResponseTo(session.IncomingMsgID, session.IncomingExternalID)

	// register to have this message committed
	session.AddPreCommitEvent(commitSessionMessages, msg)
	session.AddPostCommitEvent(sendSessionMessages, msg)

	return nil
}
