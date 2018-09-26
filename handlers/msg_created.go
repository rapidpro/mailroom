package handlers

import (
	"context"

	"github.com/apex/log"
	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/juju/errors"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/courier"
	"github.com/nyaruka/mailroom/models"
	"github.com/sirupsen/logrus"
)

func init() {
	models.RegisterEventHandler(events.TypeMsgCreated, ApplyMsgCreatedEvent)
}

// our hook for sending session messages
type SendSessionMessages struct{}

var sendSessionMessages = &SendSessionMessages{}

// SendSessionMessages sends all non-android messages to courier
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

// our hook for comitting session messages
type CommitSessionMessages struct{}

var commitSessionMessages = &CommitSessionMessages{}

// commitSessionMessages takes care of inserting all the messages in the passed in sessions assigning topups to them as needed.
func (h *CommitSessionMessages) Apply(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, sessions map[*models.Session][]interface{}) error {
	// build up a list of all the messages that need inserting
	args := make([]interface{}, 0, len(sessions))
	for _, ms := range sessions {
		for _, m := range ms {
			args = append(args, m)
		}
	}

	// find the topup we will assign
	rc := rp.Get()
	topup, err := models.DecrementOrgCredits(ctx, tx, rc, org.OrgID(), len(args))
	rc.Close()
	if err != nil {
		return errors.Annotatef(err, "error finding active topup")
	}

	// if we have an active topup, assign it to our messages
	if topup != models.NilTopupID {
		for _, m := range args {
			m.(*models.Msg).TopUpID = topup
		}
	}

	// insert all our messages
	err = models.BulkInsert(ctx, tx, models.InsertMsgSQL, args)
	if err != nil {
		return errors.Annotatef(err, "error writing messages")
	}

	return nil
}

// ApplyMsgCreatedEvent creates the db msg for the passed in event
func ApplyMsgCreatedEvent(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, session *models.Session, e flows.Event) error {
	event := e.(*events.MsgCreatedEvent)
	logrus.WithFields(logrus.Fields{
		"contact_uuid": session.ContactUUID(),
		"text":         event.Msg.Text(),
		"urn":          event.Msg.URN(),
	}).Debug("creating message")

	// get our channel
	channel := session.Org().ChannelByUUID(event.Msg.Channel().UUID)
	if channel == nil {
		return errors.Errorf("unable to load channel with uuid: %s", event.Msg.Channel().UUID)
	}

	msg, err := models.NewOutgoingMsg(ctx, tx, rp, session.Org().OrgID(), channel, session.ContactID, &event.Msg, event.CreatedOn())
	if err != nil {
		return errors.Annotatef(err, "error creating outgoing message to %s", event.Msg.URN())
	}

	// register to have this message committed
	session.AddPreCommitEvent(commitSessionMessages, msg)
	session.AddPostCommitEvent(sendSessionMessages, msg)

	return nil
}
