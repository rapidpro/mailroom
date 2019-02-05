package hooks

import (
	"context"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/models"
	"github.com/sirupsen/logrus"
)

func init() {
	models.RegisterEventHook(events.TypeContactNameChanged, handleContactNameChanged)
}

// CommitNameChangesHook is our hook for name changes
type CommitNameChangesHook struct{}

var commitNameChangesHook = &CommitNameChangesHook{}

// Apply commits our contact name changes as a bulk update for the passed in map of sessions
func (h *CommitNameChangesHook) Apply(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, sessions map[*models.Session][]interface{}) error {
	// build up our list of pairs of contact id and contact name
	updates := make([]interface{}, 0, len(sessions))
	for s, e := range sessions {
		// we only care about the last name change
		event := e[len(e)-1].(*events.ContactNameChangedEvent)
		updates = append(updates, &nameUpdate{s.ContactID(), event.Name})
	}

	// do our update
	return models.BulkSQL(ctx, "updating contact name", tx, updateContactNameSQL, updates)
}

// handleContactNameChanged changes the name of the contact
func handleContactNameChanged(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, session *models.Session, e flows.Event) error {
	event := e.(*events.ContactNameChangedEvent)
	logrus.WithFields(logrus.Fields{
		"contact_uuid": session.ContactUUID(),
		"session_id":   session.ID(),
		"name":         event.Name,
	}).Debug("changing contact name")

	session.AddPreCommitEvent(commitNameChangesHook, event)
	return nil
}

// struct used for our bulk insert
type nameUpdate struct {
	ContactID flows.ContactID `db:"id"`
	Name      string          `db:"name"`
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
