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

// UpdatePreferredURN updates the URNs for the contact (if needbe) to have the passed in URN as top priority
// with the passed in channel as the preferred channel
func (c *Contact) UpdatePreferredURN(ctx context.Context, tx Queryer, org *OrgAssets, urnID URNID, channel *Channel) error {
	// no urns? that's an error
	if len(c.urns) == 0 {
		return errors.Errorf("can't set preferred URN on contact with no URNs")
	}

	// no channel, that's an error
	if channel == nil {
		return errors.Errorf("can't set preferred channel to a nil channel")
	}

	// is this already our top URN
	topURNID := URNID(getURNInt(c.urns[0], "id"))
	topChannelID := getURNChannelID(org, c.urns[0])

	// we are already the top URN, nothing to do
	if topURNID == urnID && topChannelID != nil && *topChannelID == channel.ID() {
		return nil
	}

	// we need to build a new list, first find our URN
	topURN := urns.NilURN
	newURNs := make([]urns.URN, 0, len(c.urns))

	priority := 999
	for _, urn := range c.urns {
		id := URNID(getURNInt(urn, "id"))
		if id == urnID {
			updated, err := updateURNChannelPriority(urn, channel, 1000)
			if err != nil {
				return errors.Wrapf(err, "error updating channel on urn")
			}
			topURN = updated
		} else {
			updated, err := updateURNChannelPriority(urn, nil, priority)
			if err != nil {
				return errors.Wrapf(err, "error updating priority on urn")
			}
			newURNs = append(newURNs, updated)
			priority--
		}
	}

	if topURN == urns.NilURN {
		return errors.Errorf("unable to find urn with id: %d", urnID)
	}

	c.urns = []urns.URN{topURN}
	c.urns = append(c.urns, newURNs...)

	change := &ContactURNsChanged{
		ContactID: c.ID(),
		URNs:      c.urns,
	}

	// write our new state to the db
	err := UpdateContactURNs(ctx, tx, org, []*ContactURNsChanged{change})
	if err != nil {
		return errors.Wrapf(err, "error updating urns for contact")
	}

	err = UpdateContactModifiedOn(ctx, tx, []flows.ContactID{c.ID()})
	if err != nil {
		return errors.Wrapf(err, "error updating modified on on contact")
	}

	return nil
}

func getURNInt(urn urns.URN, key string) int {
	values, err := urn.Query()
	if err != nil {
		return 0
	}

	value, _ := strconv.Atoi(values.Get(key))
	return value
}

func getURNChannelID(org *OrgAssets, urn urns.URN) *ChannelID {
	values, err := urn.Query()
	if err != nil {
		return nil
	}

	channelUUID := values.Get("channel")
	if channelUUID == "" {
		return nil
	}

	channel := org.ChannelByUUID(assets.ChannelUUID(channelUUID))
	if channel != nil {
		channelID := channel.ID()
		return &channelID
	}
	return nil
}

func updateURNChannelPriority(urn urns.URN, channel *Channel, priority int) (urns.URN, error) {
	query, err := urn.Query()
	if err != nil {
		return urns.NilURN, errors.Errorf("error parsing query from URN: %s", urn)
	}
	if channel != nil {
		query["channel"] = []string{string(channel.UUID())}
	}
	query["priority"] = []string{strconv.FormatInt(int64(priority), 10)}

	urn, err = urns.NewURNFromParts(urn.Scheme(), urn.Path(), query.Encode(), urn.Display())
	if err != nil {
		return urns.NilURN, errors.Wrapf(err, "unable to create new urn")
	}

	return urn, nil
}

// UpdateContactModifiedOn updates modified on on the passed in contact
func UpdateContactModifiedOn(ctx context.Context, tx Queryer, contactIDs []flows.ContactID) error {
	_, err := tx.ExecContext(ctx, `UPDATE contacts_contact SET modified_on = NOW() WHERE id = ANY($1)`, pq.Array(contactIDs))
	return err
}

// UpdateContactURNs updates the contact urns in our database to match the passed in changes
func UpdateContactURNs(ctx context.Context, tx Queryer, org *OrgAssets, changes []*ContactURNsChanged) error {
	// keep track of all our inserts
	inserts := make([]interface{}, 0, len(changes))

	// and updates
	updates := make([]interface{}, 0, len(changes))

	// identities we are inserting
	identities := make([]string, 0, 1)

	// for each of our changes (one per contact)
	for _, change := range changes {
		// priority for each contact starts at 1000
		priority := 1000

		// for each of our urns
		for _, urn := range change.URNs {
			// parse our query
			query, err := urn.Query()
			if err != nil {
				return errors.Wrapf(err, "error parsing query for urn: %s", urn)
			}

			// figure out if we have a channel
			channelID := getURNChannelID(org, urn)

			// do we have an id?
			urnID := getURNInt(urn, "id")

			if urnID > 0 {
				// if so, this is a URN update
				updates = append(updates, &urnUpdate{
					URNID:     URNID(urnID),
					ChannelID: channelID,
					Priority:  priority,
				})
			} else {
				// otherwise this is a new URN insert
				var display *string
				if urn.Display() != "" {
					d := urn.Display()
					display = &d
				}

				var auth *string
				if len(query["auth"]) > 0 {
					a := query["auth"][0]
					auth = &a
				}

				// new URN, add it instead
				inserts = append(inserts, &urnInsert{
					ContactID: change.ContactID,
					Identity:  urn.Identity().String(),
					Path:      urn.Path(),
					Display:   display,
					Auth:      auth,
					Scheme:    urn.Scheme(),
					Priority:  priority,
					OrgID:     org.OrgID(),
				})

				identities = append(identities, urn.Identity().String())
			}

			// decrease our priority for the next URN
			priority--
		}
	}

	// first update existing URNs
	err := BulkSQL(ctx, "updating contact urns", tx, updateContactURNsSQL, updates)
	if err != nil {
		return errors.Wrapf(err, "error updating urns")
	}

	if len(inserts) > 0 {
		// find the unique ids of the contacts that may be affected by our URN inserts
		rows, err := tx.QueryxContext(ctx,
			`SELECT contact_id FROM contacts_contacturn WHERE identity = ANY($1) AND org_id = $2 AND contact_id IS NOT NULL`,
			pq.Array(identities), org.OrgID(),
		)
		if err != nil {
			return errors.Wrapf(err, "error finding contacts for urns")
		}
		defer rows.Close()

		orphanedIDs := make([]flows.ContactID, 0, len(inserts))
		for rows.Next() {
			var contactID flows.ContactID
			err := rows.Scan(&contactID)
			if err != nil {
				return errors.Wrapf(err, "error reading orphaned contacts")
			}
			orphanedIDs = append(orphanedIDs, contactID)
		}

		// then insert new urns, we do these one by one since we have to deal with conflicts
		for _, insert := range inserts {
			_, err := tx.NamedExecContext(ctx, insertContactURNsSQL, insert)
			if err != nil {
				return errors.Wrapf(err, "error inserting new urns")
			}
		}

		// finally mark all the orphaned contacts as modified
		if len(orphanedIDs) > 0 {
			err := UpdateContactModifiedOn(ctx, tx, orphanedIDs)
			if err != nil {
				return errors.Wrapf(err, "error updating orphaned contacts")
			}
		}
	}

	// NOTE: caller needs to update modified on for this contact
	return nil
}

// urnUpdate is our object that represents a single contact URN update
type urnUpdate struct {
	URNID     URNID      `db:"id"`
	ChannelID *ChannelID `db:"channel_id"`
	Priority  int        `db:"priority"`
}

const updateContactURNsSQL = `
UPDATE 
	contacts_contacturn u
SET
	channel_id = r.channel_id::int,
	priority = r.priority::int
FROM
	(VALUES(:id, :channel_id, :priority))
AS
	r(id, channel_id, priority)
WHERE
	u.id = r.id::int
`

// urnInsert is our object that represents a single contact URN addition
type urnInsert struct {
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
ON CONFLICT(identity, org_id)
DO 
	UPDATE
	SET 
		contact_id = :contact_id,
		priority = :priority
	`

// ContactURNsChanged represents the new status of URNs for a contact
type ContactURNsChanged struct {
	ContactID flows.ContactID
	OrgID     OrgID
	URNs      []urns.URN
}
