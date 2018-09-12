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
func ApplyEvent(ctx context.Context, tx *sqlx.Tx, org *OrgAssets, session *Session, run *FlowRun, e flows.Event) error {
	switch event := e.(type) {
	case *events.MsgCreatedEvent:
		return ApplyMsgCreatedEvent(ctx, tx, org, session, run, event)
	default:
		return nil
	}
}

// ApplyMsgCreatedEvent creates the db msg for the passed in event
func ApplyMsgCreatedEvent(ctx context.Context, tx *sqlx.Tx, org *OrgAssets, session *Session, run *FlowRun, event *events.MsgCreatedEvent) error {
	// insert our new message
	logrus.WithFields(logrus.Fields{
		"contact_id": session.ContactID,
		"text":       event.Msg.Text(),
		"urn":        event.Msg.URN(),
	}).Info("creating message")

	// get our channel id
	channel, err := org.GetChannel(event.Msg.Channel().UUID)
	if err != nil {
		return errors.Annotate(err, "error loading channel for outgoing message")
	}

	msg, err := CreateOutgoingMsg(ctx, tx, org.GetOrgID(), channel.ID(), session.ContactID, &event.Msg)
	if err != nil {
		return errors.Annotatef(err, "error creating outgoing message to %s", event.Msg.URN())
	}

	session.AddOutboxMsg(msg)
	return nil
}
