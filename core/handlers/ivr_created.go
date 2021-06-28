package handlers

import (
	"context"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/core/hooks"
	"github.com/nyaruka/mailroom/core/models"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func init() {
	models.RegisterEventHandler(events.TypeIVRCreated, handleIVRCreated)
}

// handleIVRCreated creates the db msg for the passed in event
func handleIVRCreated(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, oa *models.OrgAssets, scene *models.Scene, e flows.Event) error {
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

	msg, err := models.NewOutgoingIVR(oa.OrgID(), conn, event.Msg, event.CreatedOn())
	if err != nil {
		return errors.Wrapf(err, "error creating outgoing ivr say: %s", event.Msg.Text())
	}

	// register to have this message committed
	scene.AppendToEventPreCommitHook(hooks.CommitIVRHook, msg)

	return nil
}
