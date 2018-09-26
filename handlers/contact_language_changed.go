package handlers

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
	models.RegisterEventHandler(events.TypeContactLanguageChanged, applyContactLanguageChanged)
}

// our hook for language changes
type CommitContactLanguageChanges struct{}

var commitContactLanguageChanges = &CommitContactLanguageChanges{}

// Apply applies our contact language change before our commit
func (h *CommitContactLanguageChanges) Apply(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, sessions map[*models.Session][]interface{}) error {
	// build up our list of pairs of contact id and language name
	updates := make([]interface{}, 0, len(sessions))
	for s, e := range sessions {
		// we only care about the last name change
		event := e[len(e)-1].(*events.ContactLanguageChangedEvent)
		updates = append(updates, &languageUpdate{int64(s.ContactID), event.Language})
	}

	// do our update
	return models.BulkInsert(ctx, tx, updateContactLanguageSQL, updates)
}

// applyContactLanguageChanged is called when we process a contact language change
func applyContactLanguageChanged(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, session *models.Session, e flows.Event) error {
	event := e.(*events.ContactLanguageChangedEvent)
	logrus.WithFields(logrus.Fields{
		"contact_uuid": session.ContactUUID(),
		"name":         event.Language,
	}).Debug("changing contact language")

	session.AddPreCommitEvent(commitContactLanguageChanges, event)
	return nil
}

// struct used for our bulk update
type languageUpdate struct {
	ContactID int64  `db:"id"`
	Language  string `db:"language"`
}

const updateContactLanguageSQL = `
	UPDATE 
		contacts_contact c
	SET
		language = r.language,
		modified_on = NOW()
	FROM (
		VALUES(:id, :language)
	) AS
		r(id, language)
	WHERE
		c.id = r.id::int
`
