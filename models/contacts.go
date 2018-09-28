package models

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"time"

	"github.com/lib/pq"

	"github.com/jmoiron/sqlx"
	"github.com/juju/errors"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/excellent/types"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/utils"
	"github.com/sirupsen/logrus"
)

type ContactID int64
type FieldUUID utils.UUID

// LoadContacts loads a set of contacts for the passed in ids
func LoadContacts(ctx context.Context, db *sqlx.DB, session flows.SessionAssets, org *OrgAssets, ids []flows.ContactID) ([]*flows.Contact, error) {
	start := time.Now()

	// TODO, should we be filtering by org here too?
	rows, err := db.QueryContext(ctx, selectContactSQL, pq.Array(ids))
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
		groups := make([]assets.Group, 0, len(env.Groups))
		for _, g := range env.Groups {
			group := org.GroupByID(g)
			if group != nil {
				groups = append(groups, group)
			}
		}

		// and our URNs to URN objects
		contactURNs := make([]urns.URN, 0, len(env.URNs))
		for _, u := range env.URNs {
			var channel *Channel

			// load any channel if present
			if u.ChannelID != ChannelID(0) {
				channel = org.ChannelByID(u.ChannelID)
			}

			// we build our query from a combination of preferred channel and auth
			query := url.Values{
				"id": []string{fmt.Sprintf("%d", u.ID)},
			}
			if channel != nil {
				query["channel"] = []string{string(channel.UUID())}
			}
			if u.Auth != "" {
				query["auth"] = []string{u.Auth}
			}

			// create our URN
			urn, err := urns.NewURNFromParts(u.Scheme, u.Path, query.Encode(), u.Display)
			if err != nil {
				logrus.WithField("urn", u.Path).WithField("scheme", u.Scheme).Error("invalid URN, ignoring")
				continue
			}
			contactURNs = append(contactURNs, urn)
		}

		// grab all our org fields
		orgFields, err := org.Fields()
		if err != nil {
			return nil, errors.Annotatef(err, "error loading org fields")
		}

		// populate all values, either with nil or the real value
		values := make(map[assets.Field]*flows.Value, len(orgFields))
		for _, f := range orgFields {
			field := f.(*Field)
			cv, found := env.Fields[field.UUID()]

			if found {
				value := flows.NewValue(
					cv.Text,
					cv.Datetime,
					cv.Number,
					cv.State,
					cv.District,
					cv.Ward,
				)
				values[field] = value
			} else {
				value := flows.Value{}
				values[field] = &value
			}
		}

		// TODO: what do we do for stopped, blocked, inactive?

		// ok, create our goflow contact now
		contact, err := flows.NewContactFromAssets(
			session,
			env.UUID,
			env.ID,
			env.Name,
			env.Language,
			org.Env().Timezone(),
			env.CreatedOn,
			contactURNs,
			groups,
			values,
		)
		if err != nil {
			return nil, errors.Annotatef(err, "error creating flow contact")
		}

		contacts = append(contacts, contact)
	}

	logrus.WithField("elapsed", time.Since(start)).WithField("count", len(contacts)).Debug("loaded contacts")

	return contacts, nil
}

// FieldValue is our utility struct for the value of a field
type FieldValue struct {
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
	Fields    map[FieldUUID]*FieldValue `json:"fields"`
	Groups    []GroupID                 `json:"groups"`
	URNs      []struct {
		ID        int       `json:"id"`
		Scheme    string    `json:"scheme"`
		Path      string    `json:"path"`
		Display   string    `json:"display"`
		Auth      string    `json:"auth"`
		ChannelID ChannelID `json:"channel_id"`
	} `json:"urns"`
	ModifiedOn time.Time `json:"modified_on"`
	CreatedOn  time.Time `json:"created_on"`
}

const selectContactSQL = `
SELECT ROW_TO_JSON(r) FROM (SELECT
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
	g.groups AS groups,
	u.urns AS urns
FROM
	contacts_contact c
LEFT JOIN (
	SELECT 
		contact_id, 
		ARRAY_AGG(contactgroup_id) AS groups 
	FROM 
		contacts_contactgroup_contacts g
	WHERE
		g.contact_id = ANY($1)		
	GROUP BY 
		contact_id
) g ON c.id = g.contact_id
LEFT JOIN (
	SELECT 
		contact_id, 
		array_agg(
			json_build_object(
				'id', u.id, 
				'scheme', u.scheme,
				'path', path,
				'display', display,
            	'auth', auth,
            	'channel_id', channel_id
			) ORDER BY priority DESC, id ASC
		) as urns 
	FROM 
		contacts_contacturn u 
	WHERE
		u.contact_id = ANY($1)
	GROUP BY 
		contact_id
) u ON c.id = u.contact_id
WHERE 
	c.id = ANY($1) AND
	is_test = FALSE AND
	is_active = TRUE
) r;
`
