package hooks

import (
	"context"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/null"
)

// CommitLanguageChangesHook is our hook for language changes
var CommitLanguageChangesHook models.EventCommitHook = &commitLanguageChangesHook{}

type commitLanguageChangesHook struct{}

// Apply applies our contact language change before our commit
func (h *commitLanguageChangesHook) Apply(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, oa *models.OrgAssets, scenes map[*models.Scene][]interface{}) error {
	// build up our list of pairs of contact id and language name
	updates := make([]interface{}, 0, len(scenes))
	for s, e := range scenes {
		// we only care about the last name change
		event := e[len(e)-1].(*events.ContactLanguageChangedEvent)
		updates = append(updates, &languageUpdate{s.ContactID(), null.String(event.Language)})
	}

	// do our update
	return models.BulkQuery(ctx, "updating contact language", tx, updateContactLanguageSQL, updates)
}

// struct used for our bulk update
type languageUpdate struct {
	ContactID models.ContactID `db:"id"`
	Language  null.String      `db:"language"`
}

const updateContactLanguageSQL = `
	UPDATE 
		contacts_contact c
	SET
		language = r.language
	FROM (
		VALUES(:id, :language)
	) AS
		r(id, language)
	WHERE
		c.id = r.id::int
`
