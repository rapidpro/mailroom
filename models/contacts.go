package models

import (
	"context"
	"time"

	"github.com/nyaruka/goflow/flows"
	"github.com/sirupsen/logrus"
)

type ContactID int

const selectContactSQL = `
SELECT ROW_TO_JSON(t) FROM (
	SELECT
		id, 
		org_id, 
		uuid, 
		name, 
		language, 
		is_stopped, 
		is_blocked, 
		is_active, 
		created_on, 
		modified_on,
		(SELECT ARRAY_AGG(CASE WHEN u.display IS NULL THEN u.identity ELSE u.identity || '#' || u.display END)
		FROM (
			SELECT 
				CASE
				WHEN cu.channel_id IS NULL 
				THEN cu.identity || '?id=' || cu.id
				ELSE cu.identity || '?id=' || cu.id || '&channel=' || cuc.uuid
				END as identity,
				cu.display as display
			FROM 
				contacts_contacturn cu
				LEFT JOIN channels_channel cuc ON cu.channel_id = cuc.id
			WHERE 
				contact_id = contacts_contact.id
			ORDER BY
				cu.priority DESC, 
				cu.id ASC
                ) u
		) as urns,
		(SELECT JSONB_AGG(f.value)
	   	FROM (
			SELECT CASE
			WHEN value ? 'ward'
				THEN JSONB_BUILD_OBJECT('ward_keyword', (REGEXP_MATCHES(value ->> 'ward', '(.* > )?([^>]+)'))[2])
				ELSE '{}' :: jsonb
			END 
			|| district_value.value AS value
				FROM (
					SELECT CASE
					WHEN value ? 'district'
						THEN jsonb_build_object('district_keyword', (regexp_matches(value ->> 'district', '(.* > )?([^>]+)'))[2])
						ELSE '{}' :: jsonb
					END || state_value.value as value
						FROM (
							SELECT CASE
								WHEN value ? 'state'
									THEN jsonb_build_object('state_keyword', (regexp_matches(value ->> 'state', '(.* > )?([^>]+)'))[2])
								ELSE '{}' :: jsonb
							END || jsonb_build_object('field', key) || value as value
								FROM jsonb_each(contacts_contact.fields)
						) state_value
				) as district_value
		) as f
		) as fields,
		(SELECT array_to_json(array_agg(g))
	   	FROM (
			SELECT
				contacts_contactgroup.uuid as uuid, 
				contacts_contactgroup.name as name
			FROM 
				contacts_contactgroup_contacts, 
				contacts_contactgroup
			WHERE 
				contact_id = contacts_contact.id AND 
				contacts_contactgroup_contacts.contactgroup_id = contacts_contactgroup.id
		) g
		) as groups
	FROM 
		contacts_contact
	WHERE 
		id = $1
) t;
`

// LoadContact loads a contact for the passed in contact id and org assets
func LoadContact(o *OrgAssets, id ContactID) (*flows.Contact, error) {
	ctx, cancel := context.WithTimeout(o.ctx, time.Second*15)
	defer cancel()

	var contactJSON string
	err := o.db.GetContext(ctx, &contactJSON, selectContactSQL, id)
	if err != nil {
		logrus.WithError(err).Error("error querying contact")
		return nil, err
	}

	// load it in from our json
	contact, err := flows.ReadContact(o, []byte(contactJSON))
	if err != nil {
		logrus.WithError(err).WithField("json", contactJSON).Error("error loading contact")
	}
	return contact, err
}
