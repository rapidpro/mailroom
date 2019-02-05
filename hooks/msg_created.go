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
	models.RegisterEventHook(events.TypeMsgCreated, handleMsgCreated)
}

// SendMessagesHook is our hook for sending session messages
type SendMessagesHook struct{}

var sendMessagesHook = &SendMessagesHook{}

// Apply sends all non-android messages to courier
func (h *SendMessagesHook) Apply(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, sessions map[*models.Session][]interface{}) error {
	rc := rp.Get()
	defer rc.Close()

	for s, args := range sessions {
		// create our message array
		msgs := make([]*models.Msg, len(args))
		for i, m := range args {
			msgs[i] = m.(*models.Msg)
		}

		// if our session has a timeout, set it on our last message
		if s.Timeout() != nil && s.WaitStartedOn() != nil {
			msgs[len(msgs)-1].SetTimeout(s.ID(), *s.WaitStartedOn(), *s.Timeout())
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

// CommitMessagesHook is our hook for comitting session messages
type CommitMessagesHook struct{}

var commitMessagesHook = &CommitMessagesHook{}

// Apply takes care of inserting all the messages in the passed in sessions assigning topups to them as needed.
func (h *CommitMessagesHook) Apply(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, sessions map[*models.Session][]interface{}) error {
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

// handleMsgCreated creates the db msg for the passed in event
func handleMsgCreated(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, session *models.Session, e flows.Event) error {
	event := e.(*events.MsgCreatedEvent)

	logrus.WithFields(logrus.Fields{
		"contact_uuid": session.ContactUUID(),
		"session_id":   session.ID(),
		"text":         event.Msg.Text(),
		"urn":          event.Msg.URN(),
	}).Debug("msg created event")

	// ignore events that don't have a channel or URN set
	// TODO: maybe we should create these messages in a failed state?
	if session.SessionType() == models.MessagingFlow && (event.Msg.URN() == urns.NilURN || event.Msg.Channel() == nil) {
		return nil
	}

	// messaging flows must have urn id set on them, assert that
	if session.SessionType() == models.MessagingFlow {
		urnInt := models.GetURNInt(event.Msg.URN(), "id")
		if urnInt == 0 {
			return errors.Errorf("attempt to create messaging message with 0 id URN")
		}
	}

	// get our channel
	var channel *models.Channel

	if event.Msg.Channel() != nil {
		channel = org.ChannelByUUID(event.Msg.Channel().UUID)
		if channel == nil {
			return errors.Errorf("unable to load channel with uuid: %s", event.Msg.Channel().UUID)
		}
	}

	msg, err := models.NewOutgoingMsg(org.OrgID(), channel, session.ContactID(), event.Msg, event.CreatedOn())
	if err != nil {
		return errors.Wrapf(err, "error creating outgoing message to %s", event.Msg.URN())
	}

	// set our reply to as well (will be noop in cases when there is no incoming message)
	msg.SetResponseTo(session.IncomingMsgID(), session.IncomingMsgExternalID())

	// register to have this message committed
	session.AddPreCommitEvent(commitMessagesHook, msg)

	// we only send messaging messages
	if session.SessionType() == models.MessagingFlow {
		session.AddPostCommitEvent(sendMessagesHook, msg)
	}

	return nil
}
