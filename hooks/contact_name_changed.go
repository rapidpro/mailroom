package hooks

import (
	"context"
	"fmt"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/models"
	"github.com/sirupsen/logrus"
)

func init() {
	models.RegisterEventHandler(events.TypeContactNameChanged, handleContactNameChanged)
}

// CommitNameChangesHook is our hook for name changes
type CommitNameChangesHook struct{}

var commitNameChangesHook = &CommitNameChangesHook{}

// Apply commits our contact name changes as a bulk update for the passed in map of scene
func (h *CommitNameChangesHook) Apply(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, scenes map[*models.Scene][]interface{}) error {
	// build up our list of pairs of contact id and contact name
	updates := make([]interface{}, 0, len(scenes))
	for s, e := range scenes {
		// we only care about the last name change
		event := e[len(e)-1].(*events.ContactNameChangedEvent)
		updates = append(updates, &nameUpdate{s.ContactID(), fmt.Sprintf("%.128s", event.Name)})
	}

	// do our update
	return models.BulkSQL(ctx, "updating contact name", tx, updateContactNameSQL, updates)
}

// handleContactNameChanged changes the name of the contact
func handleContactNameChanged(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, scene *models.Scene, e flows.Event) error {
	event := e.(*events.ContactNameChangedEvent)
	logrus.WithFields(logrus.Fields{
		"contact_uuid": scene.ContactUUID(),
		"session_id":   scene.SessionID(),
		"name":         event.Name,
	}).Debug("changing contact name")

	scene.AppendToEventPreCommitHook(commitNameChangesHook, event)
	return nil
}

// struct used for our bulk insert
type nameUpdate struct {
	ContactID models.ContactID `db:"id"`
	Name      string           `db:"name"`
}

const updateContactNameSQL = `
	UPDATE 
		contacts_contact c
	SET
		name = r.name,
		modified_on = NOW()
	FROM (
		VALUES(:id, :name)
	) AS
		r(id, name)
	WHERE
		c.id = r.id::int
`
