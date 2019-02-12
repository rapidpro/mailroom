package hooks

import (
	"context"
	"encoding/json"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/models"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func init() {
	models.RegisterEventHook(events.TypeContactFieldChanged, handleContactFieldChanged)
}

// CommitFieldChangesHook is our hook for contact field changes
type CommitFieldChangesHook struct{}

var commitFieldChangesHook = &CommitFieldChangesHook{}

// Apply squashes and writes all the field updates for the contacts
func (h *CommitFieldChangesHook) Apply(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, sessions map[*models.Session][]interface{}) error {
	// our list of updates
	fieldUpdates := make([]interface{}, 0, len(sessions))
	fieldDeletes := make([]interface{}, 0)
	for session, es := range sessions {
		updates := make(map[models.FieldUUID]*flows.Value, len(es))
		for _, e := range es {
			event := e.(*events.ContactFieldChangedEvent)
			field := org.FieldByKey(event.Field.Key)
			if field == nil {
				logrus.WithFields(logrus.Fields{
					"field_key":  event.Field.Key,
					"field_name": event.Field.Name,
					"session_id": session.ID(),
				}).Debug("unable to find field with key, ignoring")
				continue
			}

			updates[field.UUID()] = event.Value
		}

		// trim out deletes, adding to our list of global deletes
		for k, v := range updates {
			if v == nil || v.Text.Native() == "" {
				delete(updates, k)
				fieldDeletes = append(fieldDeletes, &FieldDelete{
					ContactID: session.ContactID(),
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
			ContactID: session.ContactID(),
			Updates:   string(fieldJSON),
		})
	}

	// first apply our deletes
	if len(fieldDeletes) > 0 {
		err := models.BulkSQL(ctx, "deleting contact field values", tx, deleteContactFieldsSQL, fieldDeletes)
		if err != nil {
			return errors.Wrapf(err, "error deleting contact fields")
		}
	}

	// then our updates
	if len(fieldUpdates) > 0 {
		err := models.BulkSQL(ctx, "updating contact field values", tx, updateContactFieldsSQL, fieldUpdates)
		if err != nil {
			return errors.Wrapf(err, "error updating contact fields")
		}
	}

	return nil
}

// handleContactFieldChanged is called when a contact field changes
func handleContactFieldChanged(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, session *models.Session, e flows.Event) error {
	event := e.(*events.ContactFieldChangedEvent)
	logrus.WithFields(logrus.Fields{
		"contact_uuid": session.ContactUUID(),
		"session_id":   session.ID(),
		"field_key":    event.Field.Key,
		"value":        event.Value,
	}).Debug("contact field changed")

	// add our callback
	session.AddPreCommitEvent(commitFieldChangesHook, event)
	session.AddPreCommitEvent(updateCampaignEventsHook, event)

	return nil
}

type FieldDelete struct {
	ContactID models.ContactID `db:"contact_id"`
	FieldUUID models.FieldUUID `db:"field_uuid"`
}

type FieldUpdate struct {
	ContactID models.ContactID `db:"contact_id"`
	Updates   string           `db:"updates"`
}

type FieldValue struct {
	Text string `json:"text"`
}

const updateContactFieldsSQL = `
UPDATE 
	contacts_contact c
SET
	fields = COALESCE(fields,'{}'::jsonb) || r.updates::jsonb,
	modified_on = NOW()
FROM (
	VALUES(:contact_id, :updates)
) AS
	r(contact_id, updates)
WHERE
	c.id = r.contact_id::int
`

const deleteContactFieldsSQL = `
UPDATE 
	contacts_contact c
SET
	fields = fields - r.field_uuid,
	modified_on = NOW()
FROM (
	VALUES(:contact_id, :field_uuid)
) AS
	r(contact_id, field_uuid)
WHERE
	c.id = r.contact_id::int
`
