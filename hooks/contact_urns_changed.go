package hooks

import (
	"context"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/models"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func init() {
	models.RegisterEventHandler(events.TypeContactURNsChanged, handleContactURNsChanged)
}

// CommitURNChangesHook is our hook for when a URN is added to a contact
type CommitURNChangesHook struct{}

var commitURNChangesHook = &CommitURNChangesHook{}

// Apply adds all our URNS in a batch
func (h *CommitURNChangesHook) Apply(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, scenes map[*models.Scene][]interface{}) error {
	// gather all our urn changes, we only care about the last change for each scene
	changes := make([]*models.ContactURNsChanged, 0, len(scenes))
	for _, sessionChanges := range scenes {
		changes = append(changes, sessionChanges[len(sessionChanges)-1].(*models.ContactURNsChanged))
	}

	err := models.UpdateContactURNs(ctx, tx, org, changes)
	if err != nil {
		return errors.Wrapf(err, "error updating contact urns")
	}

	return nil
}

// handleContactURNsChanged is called for each contact urn changed event that is encountered
func handleContactURNsChanged(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, scene *models.Scene, e flows.Event) error {
	event := e.(*events.ContactURNsChangedEvent)
	logrus.WithFields(logrus.Fields{
		"contact_uuid": scene.ContactUUID(),
		"session_id":   scene.SessionID(),
		"urns":         event.URNs,
	}).Debug("contact urns changed")

	// create our URN changed event
	change := &models.ContactURNsChanged{
		ContactID: scene.ContactID(),
		OrgID:     org.OrgID(),
		URNs:      event.URNs,
	}

	// add our callback
	scene.AppendToEventPreCommitHook(commitURNChangesHook, change)
	scene.AppendToEventPreCommitHook(contactModifiedHook, scene.Contact().ID())

	return nil
}
