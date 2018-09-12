package models

import (
	"context"
	"encoding/json"
	"net/url"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/juju/errors"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/excellent/types"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/utils"
)

type FieldUUID utils.UUID

const selectContactsSQL = `
SELECT ROW_TO_JSON(t) FROM (SELECT
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
	fields,
	(SELECT ARRAY_AGG(u) FROM (
		SELECT
            cu.scheme as scheme,
            cu.path as path,
            cu.display as display,
            cu.auth as auth,
            cu.channel_id as channel_id
		FROM 
    		contacts_contacturn cu
		WHERE 
			contact_id = contacts_contact.id
		ORDER BY
			cu.priority DESC, 
			cu.id ASC
    ) u) as urns,
	(SELECT ARRAY_AGG(g.group_id) FROM (
		SELECT
			cg.contactgroup_id as group_id
		FROM 
			contacts_contactgroup_contacts cg
			LEFT JOIN contacts_contactgroup g ON cg.contactgroup_id = g.id
		WHERE 
			contact_id = contacts_contact.id AND
			g.group_type = 'U' AND
			g.is_active = TRUE
	) g) as groups
FROM 
	contacts_contact
WHERE 
	id IN (?)
) t;
`

// LoadContacts loads a set of contacts for the passed in ids
func LoadContacts(ctx context.Context, db *sqlx.DB, o *OrgAssets, ids []flows.ContactID) ([]*flows.Contact, error) {
	// rebind our query for our IN clause
	q, vs, err := sqlx.In(selectContactsSQL, ids)
	if err != nil {
		return nil, errors.Annotate(err, "error rebinding contacts query")
	}
	q = db.Rebind(q)

	rows, err := db.QueryContext(ctx, q, vs...)
	if err != nil {
		return nil, errors.Annotate(err, "error selecting contacts")
	}
	defer rows.Close()

	contacts := make([]*flows.Contact, 0, len(ids))
	for rows.Next() {
		env := contactEnvelope{}
		contactJSON := ""

		err = rows.Scan(&contactJSON)
		if err != nil {
			return nil, errors.Annotate(err, "error scanning contact json")
		}

		err := json.Unmarshal([]byte(contactJSON), &env)
		if err != nil {
			return nil, errors.Annotatef(err, "error unmarshalling contact json: %s", contactJSON)
		}

		// convert our group ids to real groups
		groups := make([]*flows.Group, 0, len(env.Groups))
		for _, g := range env.Groups {
			group, err := o.GetGroupByID(g)
			if err != nil {
				return nil, errors.Annotatef(err, "error loading group: %d", g)
			}
			if group != nil {
				groups = append(groups, group)
			}
		}

		// and our URNs to URN objects
		contactURNs := make(flows.URNList, 0, len(env.URNs))
		for _, u := range env.URNs {
			var channel flows.Channel

			// load any channel if present
			if u.ChannelID != flows.ChannelID(0) {
				channel, err = o.GetChannelByID(u.ChannelID)
				if err != nil {
					return nil, errors.Annotatef(err, "error loading channel: %d", u.ChannelID)
				}
			}

			// we build our query from a combination of preferred channel and auth
			query := url.Values{}
			if channel != nil {
				query["channel"] = []string{string(channel.UUID())}
			}
			if u.Auth != "" {
				query["auth"] = []string{u.Auth}
			}

			// create our URN
			urn, err := urns.NewURNFromParts(u.Scheme, u.Path, query.Encode(), u.Display)
			if err != nil {
				return nil, errors.Annotatef(err, "error loading contact, invalid urn: %s %s %s %s", u.Scheme, u.Path, query.Encode(), u.Display)
			}

			contactURNs = append(contactURNs, flows.NewContactURN(urn, channel))
		}

		// first populate all the fields with empty fields
		fields, err := o.GetFieldSet()
		if err != nil {
			return nil, errors.Annotatef(err, "error loading fields for org")
		}
		values := make(flows.FieldValues, len(fields.All()))
		for _, f := range fields.All() {
			values[f.Key()] = flows.NewEmptyFieldValue(f)
		}

		// then populate those fields that are actually set
		for uuid, value := range env.Fields {
			field, err := o.GetFieldByUUID(uuid)
			if err != nil {
				return nil, errors.Annotatef(err, "error loading field for uuid: %s", uuid)
			}
			value := flows.NewFieldValue(
				field,
				value.Text,
				value.Datetime,
				value.Number,
				value.State,
				value.District,
				value.Ward,
			)

			values[field.Key()] = value
		}

		// TODO: load real timezone for contact (same as org)
		// TODO: what do we do for stopped, blocked, inactive?

		// ok, create our goflow contact now
		contact := flows.NewContact(
			env.UUID,
			env.ID,
			env.Name,
			env.Language,
			time.UTC,
			env.CreatedOn,
			contactURNs,
			flows.NewGroupList(groups),
			values,
		)
		contacts = append(contacts, contact)
	}
	return contacts, nil
}

// utility struct for the value of a field
type fieldValue struct {
	Text     types.XText        `json:"text"`
	Datetime *types.XDateTime   `json:"datetime,omitempty"`
	Number   *types.XNumber     `json:"number,omitempty"`
	State    flows.LocationPath `json:"state,omitempty"`
	District flows.LocationPath `json:"district,omitempty"`
	Ward     flows.LocationPath `json:"ward,omitempty"`
}

// utility struct we use when reading contacts from SQL
type contactEnvelope struct {
	UUID      flows.ContactUUID         `json:"uuid"`
	ID        flows.ContactID           `json:"id"`
	Name      string                    `json:"name"`
	Language  utils.Language            `json:"language"`
	IsStopped bool                      `json:"is_stopped"`
	IsBlocked bool                      `json:"is_blocked"`
	IsActive  bool                      `json:"is_active"`
	Fields    map[FieldUUID]*fieldValue `json:"fields"`
	Groups    []flows.GroupID           `json:"groups"`
	URNs      []struct {
		Scheme    string          `json:"scheme"`
		Path      string          `json:"path"`
		Display   string          `json:"display"`
		Auth      string          `json:"auth"`
		ChannelID flows.ChannelID `json:"channel_id"`
	} `json:"urns"`
	ModifiedOn time.Time `json:"modified_on"`
	CreatedOn  time.Time `json:"created_on"`
}
