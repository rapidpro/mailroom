package handlers

import (
	"context"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/core/hooks"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func init() {
	models.RegisterEventHandler(events.TypeIVRCreated, handleIVRCreated)
}

// handleIVRCreated creates the db msg for the passed in event
func handleIVRCreated(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, oa *models.OrgAssets, scene *models.Scene, e flows.Event) error {
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

	msg := models.NewOutgoingIVR(rt.Config, oa.OrgID(), conn, event.Msg, event.CreatedOn())

	// register to have this message committed
	scene.AppendToEventPreCommitHook(hooks.CommitIVRHook, msg)

	return nil
}
