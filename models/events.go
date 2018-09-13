package models

import (
	"context"

	"github.com/jmoiron/sqlx"
	"github.com/juju/errors"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/sirupsen/logrus"
)

// ApplyEvent applies the passed in event, IE, creates the db objects required etc..
func ApplyEvent(ctx context.Context, tx *sqlx.Tx, track *Track, session *Session, run *FlowRun, e flows.Event) error {
	switch event := e.(type) {
	case *events.MsgCreatedEvent:
		return ApplyMsgCreatedEvent(ctx, tx, track, session, run, event)
	default:
		return nil
	}
}

// ApplyMsgCreatedEvent creates the db msg for the passed in event
func ApplyMsgCreatedEvent(ctx context.Context, tx *sqlx.Tx, track *Track, session *Session, run *FlowRun, event *events.MsgCreatedEvent) error {
	// insert our new message
	logrus.WithFields(logrus.Fields{
		"contact_id": session.ContactID,
		"text":       event.Msg.Text(),
		"urn":        event.Msg.URN(),
	}).Info("creating message")

	// get our channel
	channel := track.Org().ChannelByUUID(event.Msg.Channel().UUID)
	if channel == nil {
		return errors.Errorf("unable to load channel with uuid: %s", event.Msg.Channel().UUID)
	}

	msg, err := CreateOutgoingMsg(ctx, tx, track.Org().OrgID(), channel.ID(), session.ContactID, &event.Msg)
	if err != nil {
		return errors.Annotatef(err, "error creating outgoing message to %s", event.Msg.URN())
	}

	session.AddOutboxMsg(msg)
	return nil
}
