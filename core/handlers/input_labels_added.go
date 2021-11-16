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
	models.RegisterEventHandler(events.TypeInputLabelsAdded, handleInputLabelsAdded)
}

// handleInputLabelsAdded is called for each input labels added event in a scene
func handleInputLabelsAdded(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, oa *models.OrgAssets, scene *models.Scene, e flows.Event) error {
	if scene.Session() == nil {
		return errors.Errorf("cannot add label, not in a session")
	}

	event := e.(*events.InputLabelsAddedEvent)
	logrus.WithFields(logrus.Fields{
		"contact_uuid": scene.ContactUUID(),
		"session_id":   scene.SessionID(),
		"labels":       event.Labels,
	}).Debug("input labels added")

	// if the sprint had input, then it was started by a msg event and we should have the message ID saved on the session
	inputMsgID := scene.Session().IncomingMsgID()
	if inputMsgID == models.NilMsgID {
		return errors.New("handling input labels added event in session without msg")
	}

	// for each label add an insertion
	for _, l := range event.Labels {
		label := oa.LabelByUUID(l.UUID)
		if label == nil {
			return errors.Errorf("unable to find label with UUID: %s", l.UUID)
		}

		scene.AppendToEventPreCommitHook(hooks.CommitAddedLabelsHook, &models.MsgLabelAdd{MsgID: inputMsgID, LabelID: label.ID()})
	}

	return nil
}
