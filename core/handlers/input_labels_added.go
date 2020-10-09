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
	models.RegisterEventHandler(events.TypeInputLabelsAdded, handleInputLabelsAdded)
}

// handleInputLabelsAdded is called for each input labels added event in a scene
func handleInputLabelsAdded(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, oa *models.OrgAssets, scene *models.Scene, e flows.Event) error {
	event := e.(*events.InputLabelsAddedEvent)
	logrus.WithFields(logrus.Fields{
		"contact_uuid": scene.ContactUUID(),
		"session_id":   scene.SessionID(),
		"labels":       event.Labels,
	}).Debug("input labels added")

	// for each label add an insertion
	for _, l := range event.Labels {
		label := oa.LabelByUUID(l.UUID)
		if label == nil {
			return errors.Errorf("unable to find label with UUID: %s", l.UUID)
		}

		if scene.Session() == nil {
			return errors.Errorf("cannot add label, not in a session")
		}

		if scene.Session().IncomingMsgID() == models.NilMsgID {
			return errors.Errorf("cannot add label, no incoming message for scene: %d", scene.SessionID())
		}

		scene.AppendToEventPreCommitHook(hooks.CommitAddedLabelsHook, &models.MsgLabelAdd{
			MsgID:   scene.Session().IncomingMsgID(),
			LabelID: label.ID(),
		})
	}

	return nil
}
