package hooks

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/pkg/errors"
)

// CommitFieldChangesHook is our hook for contact field changes
var CommitFieldChangesHook models.EventCommitHook = &commitFieldChangesHook{}

type commitFieldChangesHook struct{}

// Apply squashes and writes all the field updates for the contacts
func (h *commitFieldChangesHook) Apply(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, oa *models.OrgAssets, scenes map[*models.Scene][]any) error {
	// our list of updates
	fieldUpdates := make([]any, 0, len(scenes))
	fieldDeletes := make(map[assets.FieldUUID][]any)
	for scene, es := range scenes {
		updates := make(map[assets.FieldUUID]*flows.Value, len(es))
		for _, e := range es {
			event := e.(*events.ContactFieldChangedEvent)
			field := oa.FieldByKey(event.Field.Key)
			if field == nil {
				slog.Debug("unable to find field with key, ignoring",
					"field_key", event.Field.Key,
					"field_name", event.Field.Name,
					"session_id", scene.SessionID(),
				)
				continue
			}

			updates[field.UUID()] = event.Value
		}

		// trim out deletes, adding to our list of global deletes
		for k, v := range updates {
			if v == nil || v.Text.Native() == "" {
				delete(updates, k)
				fieldDeletes[k] = append(fieldDeletes[k], &FieldDelete{
					ContactID: scene.ContactID(),
					FieldUUID: k,
				})
			}
		}

		// marshal the rest of our updates to JSON
		fieldJSON, err := json.Marshal(updates)
		if err != nil {
			return errors.Wrapf(err, "error marshalling field values")
		}

		// and queue them up for our update
		fieldUpdates = append(fieldUpdates, &FieldUpdate{
			ContactID: scene.ContactID(),
			Updates:   string(fieldJSON),
		})
	}

	// first apply our deletes
	// in pg9.6 we need to do this as one query per field type, in pg10 we can rewrite this to be a single query
	for _, fds := range fieldDeletes {
		err := models.BulkQuery(ctx, "deleting contact field values", tx, sqlDeleteContactFields, fds)
		if err != nil {
			return errors.Wrapf(err, "error deleting contact fields")
		}
	}

	// then our updates
	if len(fieldUpdates) > 0 {
		err := models.BulkQuery(ctx, "updating contact field values", tx, sqlUpdateContactFields, fieldUpdates)
		if err != nil {
			return errors.Wrapf(err, "error updating contact fields")
		}
	}

	return nil
}

type FieldDelete struct {
	ContactID models.ContactID `db:"contact_id"`
	FieldUUID assets.FieldUUID `db:"field_uuid"`
}

type FieldUpdate struct {
	ContactID models.ContactID `db:"contact_id"`
	Updates   string           `db:"updates"`
}

type FieldValue struct {
	Text string `json:"text"`
}

const sqlUpdateContactFields = `
UPDATE contacts_contact c
   SET fields = COALESCE(fields,'{}'::jsonb) || r.updates::jsonb
  FROM (VALUES(:contact_id, :updates)) AS r(contact_id, updates)
 WHERE c.id = r.contact_id::int`

const sqlDeleteContactFields = `
UPDATE contacts_contact c
   SET fields = fields - r.field_uuid
  FROM (VALUES(:contact_id, :field_uuid)) AS r(contact_id, field_uuid)
 WHERE c.id = r.contact_id::int`
