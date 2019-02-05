package hooks

import (
	"context"
	"fmt"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/models"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func init() {
	models.RegisterEventHook(events.TypeInputLabelsAdded, handleInputLabelsAdded)
}

// CommitAddedLabelsHook is our hook for input labels being added
type CommitAddedLabelsHook struct{}

var commitAddedLabelsHook = &CommitAddedLabelsHook{}

// Apply applies our input labels added, committing them in a single batch
func (h *CommitAddedLabelsHook) Apply(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, sessions map[*models.Session][]interface{}) error {
	// build our list of msg label adds, we dedupe these so we never double add in the same transaction
	seen := make(map[string]bool)
	adds := make([]*models.MsgLabelAdd, 0, len(sessions))

	for _, as := range sessions {
		for _, a := range as {
			add := a.(*models.MsgLabelAdd)
			key := fmt.Sprintf("%d:%d", add.LabelID, add.MsgID)
			if !seen[key] {
				adds = append(adds, add)
				seen[key] = true
			}
		}
	}

	// insert our adds
	return models.AddMsgLabels(ctx, tx, adds)
}

// handleInputLabelsAdded is called for each input labels added event in a session
func handleInputLabelsAdded(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, session *models.Session, e flows.Event) error {
	event := e.(*events.InputLabelsAddedEvent)
	logrus.WithFields(logrus.Fields{
		"contact_uuid": session.ContactUUID(),
		"session_id":   session.ID(),
		"labels":       event.Labels,
	}).Debug("input labels added")

	// for each label add an insertion
	for _, l := range event.Labels {
		label := org.LabelByUUID(l.UUID)
		if label == nil {
			return errors.Errorf("unable to find label with UUID: %s", l.UUID)
		}

		if session.IncomingMsgID().IsZero() {
			return errors.Errorf("cannot add label, no incoming message for session: %d", session.ID())
		}

		session.AddPreCommitEvent(commitAddedLabelsHook, &models.MsgLabelAdd{
			MsgID:   flows.MsgID(session.IncomingMsgID().Int64),
			LabelID: label.ID(),
		})
	}

	return nil
}
