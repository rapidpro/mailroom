package models

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/lib/pq"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/excellent/types"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/utils"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type URNID int

var NilURNID = URNID(0)

// LoadContacts loads a set of contacts for the passed in ids
func LoadContacts(ctx context.Context, db *sqlx.DB, org *OrgAssets, ids []flows.ContactID) ([]*Contact, error) {
	start := time.Now()

	rows, err := db.QueryxContext(ctx, selectContactSQL, pq.Array(ids))
	if err != nil {
		return nil, errors.Wrap(err, "error selecting contacts")
	}
	defer rows.Close()

	contacts := make([]*Contact, 0, len(ids))
	for rows.Next() {
		e := &contactEnvelope{}
		err := readJSONRow(rows, e)
		if err != nil {
			return nil, errors.Wrap(err, "error scanning contact json")
		}

		contact := &Contact{
			id:         e.ID,
			uuid:       e.UUID,
			name:       e.Name,
			language:   e.Language,
			isStopped:  e.IsStopped,
			isBlocked:  e.IsBlocked,
			modifiedOn: e.ModifiedOn,
			createdOn:  e.CreatedOn,
		}

		// load our real groups
		groups := make([]assets.Group, 0, len(e.GroupIDs))
		for _, g := range e.GroupIDs {
			group := org.GroupByID(g)
			if group != nil {
				groups = append(groups, group)
			}
		}
		contact.groups = groups

		// create our map of field values filtered by what we know exists
		fields := make(map[string]*flows.Value)
		orgFields, _ := org.Fields()
		for _, f := range orgFields {
			field := f.(*Field)
			cv, found := e.Fields[field.UUID()]
			if found {
				value := flows.NewValue(
					cv.Text,
					cv.Datetime,
					cv.Number,
					cv.State,
					cv.District,
					cv.Ward,
				)
				fields[field.Key()] = value
			}
		}
		contact.fields = fields

		// finally build up our URN objects
		contactURNs := make([]urns.URN, 0, len(e.URNs))
		for _, u := range e.URNs {
			var channel *Channel

			// load any channel if present
			if u.ChannelID != ChannelID(0) {
				channel = org.ChannelByID(u.ChannelID)
			}

			// we build our query from a combination of preferred channel and auth
			query := url.Values{
				"id":       []string{fmt.Sprintf("%d", u.ID)},
				"priority": []string{fmt.Sprintf("%d", u.Priority)},
			}
			if channel != nil {
				query["channel"] = []string{string(channel.UUID())}
				query["channel_id"] = []string{fmt.Sprintf("%d", channel.ID())}
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
		contact.urns = contactURNs

		contacts = append(contacts, contact)
	}

	logrus.WithField("elapsed", time.Since(start)).WithField("count", len(contacts)).Debug("loaded contacts")

	return contacts, nil
}

// FlowContact converts our mailroom contact into a flow contact for use in the engine
func (c *Contact) FlowContact(org *OrgAssets, session flows.SessionAssets) (*flows.Contact, error) {
	// create our flow contact
	contact, err := flows.NewContactFromAssets(
		session,
		c.uuid,
		c.id,
		c.name,
		c.language,
		org.Env().Timezone(),
		c.createdOn,
		c.urns,
		c.groups,
		c.fields,
	)
	if err != nil {
		return nil, errors.Wrapf(err, "error creating flow contact")
	}

	return contact, nil
}

// Unstop sets the is_stopped attribute to false for this contact
func (c *Contact) Unstop(ctx context.Context, db *sqlx.DB) error {
	_, err := db.ExecContext(ctx, `UPDATE contacts_contact SET is_stopped = FALSE, modified_on = NOW() WHERE id = $1`, c.id)
	if err != nil {
		return errors.Wrapf(err, "error unstopping contact")
	}
	c.isStopped = false
	return nil
}

// UpdatePreferredURNAndChannel updates the URNs for the contact (if needbe) to have the passed in URN as top
// priority with the passed in channel as the preferred channel
func (c *Contact) UpdatePreferredURNAndChannel(ctx context.Context, db *sqlx.DB, urnID URNID, channel *Channel) error {
	// no urns? no op
	if len(c.urns) == 0 {
		return errors.Errorf("can't set preferred URN on contact with no URNs")
	}

	// no channel, that's an error
	if channel == nil {
		return errors.Errorf("can't set preferred channel to a nil channel")
	}

	// whether this is a tel channel
	isTelChannel := false
	for _, s := range channel.Schemes() {
		if s == urns.TelScheme {
			isTelChannel = true
			break
		}
	}

	// if this isn't our highest priority URN, reorder our urn
	changed := make([]urns.URN, 0, 1)
	if URNID(getURNInt(c.urns[0], "id")) != urnID {
		topPriority := getURNInt(c.urns[0], "priority") + 1
		topURN := urns.NilURN
		otherURNs := make([]urns.URN, 0, len(c.urns))
		for _, u := range c.urns {
			if URNID(getURNInt(u, "id")) == urnID {
				u, err := updateURNChannelPriority(u, channel, topPriority)
				if err != nil {
					return errors.Wrapf(err, "unable to update URN: %s", u)
				}
				topURN = u
				changed = append(changed, u)
			} else {
				otherURNs = append(otherURNs, u)
			}
		}
		c.urns = append([]urns.URN{topURN}, otherURNs...)
	}

	// now do a pass to see if any of our tel URNs need to have their channel affinity changed
	if isTelChannel {
		for i, u := range c.urns {
			if isTelChannel && u.Scheme() == urns.TelScheme {
				u, err := updateURNChannelPriority(u, channel, getURNInt(u, "priority"))
				if err != nil {
					return errors.Wrapf(err, "unable to update URN: %s", u)
				}
				c.urns[i] = u
				changed = append(changed, u)
			}
		}
	}

	// finally build up our list of updates
	updates := make([]interface{}, 0, len(changed))
	for _, u := range changed {
		id := getURNInt(u, "id")
		channelID := getURNInt(u, "channel_id")
		priority := getURNInt(u, "priority")
		if id == 0 || channelID == 0 {
			return errors.Errorf("unable to read id or channel_id from URN: %s", u)
		}
		updates = append(updates, urnUpdate{URNID: URNID(id), ChannelID: ChannelID(channelID), Priority: priority})
	}

	// commit them
	err := BulkSQL(ctx, "updating preferred URN", db, updateURNs, updates)
	if err != nil {
		return errors.Wrapf(err, "error committing urn update")
	}

	// TODO: if we did anything, update our modified_on as well
	return nil
}

const updateURNs = `
	UPDATE 
		contacts_contacturn
	SET
		channel_id = r.channel_id::int,
		priority = r.priority::int
	FROM (
		VALUES(:id, :channel_id, :priority)
	) AS
		r(id, channel_id, priority)
	WHERE
		contacts_contacturn.id = r.id::int
`

func getURNInt(urn urns.URN, key string) int {
	_, _, query, _ := urn.ToParts()
	parsedQuery, err := url.ParseQuery(query)
	if err != nil {
		return 0
	}

	value, _ := strconv.Atoi(parsedQuery.Get(key))
	return value
}

func updateURNChannelPriority(urn urns.URN, channel *Channel, priority int) (urns.URN, error) {
	scheme, path, query, display := urn.ToParts()
	parsedQuery, err := url.ParseQuery(query)
	if err != nil {
		return urns.NilURN, errors.Errorf("error parsing query from URN: %s", urn)
	}
	parsedQuery["priority"] = []string{fmt.Sprintf("%d", priority)}
	parsedQuery["channel"] = []string{string(channel.UUID())}
	parsedQuery["channel_id"] = []string{fmt.Sprintf("%d", channel.ID())}

	urn, err = urns.NewURNFromParts(scheme, path, parsedQuery.Encode(), display)
	if err != nil {
		return urns.NilURN, errors.Wrapf(err, "unable to create new urn")
	}

	return urn, nil
}

type urnUpdate struct {
	URNID     URNID     `db:"id"`
	ChannelID ChannelID `db:"channel_id"`
	Priority  int       `db:"priority"`
}

// Contact is our mailroom struct that represents a contact
type Contact struct {
	id         flows.ContactID
	uuid       flows.ContactUUID
	name       string
	language   utils.Language
	isStopped  bool
	isBlocked  bool
	fields     map[string]*flows.Value
	groups     []assets.Group
	urns       []urns.URN
	modifiedOn time.Time
	createdOn  time.Time
}

func (c *Contact) ID() flows.ContactID             { return c.id }
func (c *Contact) UUID() flows.ContactUUID         { return c.uuid }
func (c *Contact) Name() string                    { return c.name }
func (c *Contact) Language() utils.Language        { return c.language }
func (c *Contact) IsStopped() bool                 { return c.isStopped }
func (c *Contact) IsBlocked() bool                 { return c.isBlocked }
func (c *Contact) Fields() map[string]*flows.Value { return c.fields }
func (c *Contact) Groups() []assets.Group          { return c.groups }
func (c *Contact) URNs() []urns.URN                { return c.urns }
func (c *Contact) ModifiedOn() time.Time           { return c.modifiedOn }
func (c *Contact) CreatedOn() time.Time            { return c.createdOn }

// fieldValueEnvelope is our utility struct for the value of a field
type fieldValueEnvelope struct {
	Text     types.XText        `json:"text"`
	Datetime *types.XDateTime   `json:"datetime,omitempty"`
	Number   *types.XNumber     `json:"number,omitempty"`
	State    flows.LocationPath `json:"state,omitempty"`
	District flows.LocationPath `json:"district,omitempty"`
	Ward     flows.LocationPath `json:"ward,omitempty"`
}

// contactEnvelope is our JSON structure for a contact as read from the database
type contactEnvelope struct {
	ID        flows.ContactID                   `json:"id"`
	UUID      flows.ContactUUID                 `json:"uuid"`
	Name      string                            `json:"name"`
	Language  utils.Language                    `json:"language"`
	IsStopped bool                              `json:"is_stopped"`
	IsBlocked bool                              `json:"is_blocked"`
	Fields    map[FieldUUID]*fieldValueEnvelope `json:"fields"`
	GroupIDs  []GroupID                         `json:"group_ids"`
	URNs      []struct {
		ID        URNID     `json:"id"`
		Priority  int       `json:"priority"`
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
	g.groups AS group_ids,
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
				'channel_id', channel_id,
				'priority', priority
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

// StopContact stops the contact with the passed in id, removing them from all groups and setting
// their state to stopped.
func StopContact(ctx context.Context, tx *sqlx.Tx, orgID OrgID, contactID flows.ContactID) error {
	// delete the contact from all groups
	_, err := tx.ExecContext(ctx, deleteAllContactGroupsSQL, orgID, contactID)
	if err != nil {
		return errors.Wrapf(err, "error removing stopped contact from groups")
	}

	// remove the contact from any triggers
	// TODO: this could leave a trigger with no contacts or groups
	_, err = tx.ExecContext(ctx, deleteAllContactTriggersSQL, contactID)
	if err != nil {
		return errors.Wrapf(err, "error removing contact from triggers")
	}

	// mark as stopped
	_, err = tx.ExecContext(ctx, markContactStoppedSQL, contactID)
	if err != nil {
		return errors.Wrapf(err, "error marking contact as stopped")
	}

	return nil
}

const deleteAllContactGroupsSQL = `
DELETE FROM
	contacts_contactgroup_contacts
WHERE
	contact_id = $2 AND
	contactgroup_id = (SELECT id from contacts_contactgroup WHERE org_id = $1 and group_type = 'U')
`

const deleteAllContactTriggersSQL = `
DELETE FROM
	triggers_trigger_contacts
WHERE
	contact_id = $1
`

const markContactStoppedSQL = `
UPDATE
	contacts_contact
SET
	is_stopped = TRUE,
	modified_on = NOW()
WHERE 
	id = $1
`

// AddContactURNs adds all the passed in contact urns in a single query
func AddContactURNs(ctx context.Context, tx *sqlx.Tx, adds []*ContactURNAdd) error {
	// convert to interface
	is := make([]interface{}, len(adds))
	for i := range adds {
		is[i] = adds[i]
	}

	// and bulk insert
	return BulkSQL(ctx, "inserting contact urns", tx, insertContactURNsSQL, is)
}

// ContactURNAdd is our object that represents a single contact URN addition
type ContactURNAdd struct {
	ContactID flows.ContactID `db:"contact_id"`
	Identity  string          `db:"identity"`
	Path      string          `db:"path"`
	Display   *string         `db:"display"`
	Auth      *string         `db:"auth"`
	Scheme    string          `db:"scheme"`
	Priority  int             `db:"priority"`
	OrgID     OrgID           `db:"org_id"`
}

const insertContactURNsSQL = `
INSERT INTO
	contacts_contacturn(contact_id, identity, path, display, auth, scheme, priority, org_id)
                 VALUES(:contact_id, :identity, :path, :display, :auth, :scheme, :priority, :org_id)
`
