package models

import (
	"context"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/sirupsen/logrus"
)

func ApplyEvent(ctx context.Context, tx *sqlx.Tx, org *OrgAssets, session *Session, run *FlowRun, e flows.Event) error {
	switch event := e.(type) {
	case *events.MsgCreatedEvent:
		return ApplyMsgCreatedEvent(ctx, tx, org, session, run, event)
	default:
		return nil
	}
}

func ApplyMsgCreatedEvent(ctx context.Context, tx *sqlx.Tx, org *OrgAssets, session *Session, run *FlowRun, event *events.MsgCreatedEvent) error {
	// insert our new message
	logrus.WithFields(logrus.Fields{
		"text": event.Msg.Text(),
		"urn":  event.Msg.URN(),
	}).Info("creating message")

	msg, err := CreateOutgoingMsg(ctx, tx, org, session.ContactID, &event.Msg)
	session.AddOutboxMsg(msg)

	return err
}
