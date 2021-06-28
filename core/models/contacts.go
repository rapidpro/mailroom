package models

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/goflow/excellent/types"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/utils/dbutil"
	"github.com/nyaruka/null"

	"github.com/lib/pq"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// URNID is our type for urn ids, which can be null
type URNID null.Int

// ContactID is our type for contact ids, which can be null
type ContactID null.Int

// URN priority constants
const (
	topURNPriority     = 1000
	defaultURNPriority = 0
)

// nil versions of ID types
const (
	NilURNID     = URNID(0)
	NilContactID = ContactID(0)
)

// ContactStatus is the type for contact statuses
type ContactStatus string

// NilContactStatus is an unset contact status
const NilContactStatus ContactStatus = ""

// possible contact statuses
const (
	ContactStatusActive   = "A"
	ContactStatusBlocked  = "B"
	ContactStatusStopped  = "S"
	ContactStatusArchived = "V"
)

var contactToModelStatus = map[flows.ContactStatus]ContactStatus{
	flows.ContactStatusActive:   ContactStatusActive,
	flows.ContactStatusBlocked:  ContactStatusBlocked,
	flows.ContactStatusStopped:  ContactStatusStopped,
	flows.ContactStatusArchived: ContactStatusArchived,
}

var contactToFlowStatus = map[ContactStatus]flows.ContactStatus{
	ContactStatusActive:   flows.ContactStatusActive,
	ContactStatusBlocked:  flows.ContactStatusBlocked,
	ContactStatusStopped:  flows.ContactStatusStopped,
	ContactStatusArchived: flows.ContactStatusArchived,
}

// Contact is our mailroom struct that represents a contact
type Contact struct {
	id         ContactID
	uuid       flows.ContactUUID
	name       string
	language   envs.Language
	status     ContactStatus
	fields     map[string]*flows.Value
	groups     []*Group
	urns       []urns.URN
	createdOn  time.Time
	modifiedOn time.Time
	lastSeenOn *time.Time
}

func (c *Contact) ID() ContactID                   { return c.id }
func (c *Contact) UUID() flows.ContactUUID         { return c.uuid }
func (c *Contact) Name() string                    { return c.name }
func (c *Contact) Language() envs.Language         { return c.language }
func (c *Contact) Status() ContactStatus           { return c.status }
func (c *Contact) Fields() map[string]*flows.Value { return c.fields }
func (c *Contact) Groups() []*Group                { return c.groups }
func (c *Contact) URNs() []urns.URN                { return c.urns }
func (c *Contact) CreatedOn() time.Time            { return c.createdOn }
func (c *Contact) ModifiedOn() time.Time           { return c.modifiedOn }
func (c *Contact) LastSeenOn() *time.Time          { return c.lastSeenOn }

// URNForID returns the flow URN for the passed in URN, return NilURN if not found
func (c *Contact) URNForID(urnID URNID) urns.URN {
	for _, u := range c.urns {
		if GetURNID(u) == urnID {
			return u
		}
	}

	return urns.NilURN
}

// Unstop sets the status to stopped for this contact
func (c *Contact) Unstop(ctx context.Context, db Queryer) error {
	_, err := db.ExecContext(ctx, `UPDATE contacts_contact SET status = 'A', modified_on = NOW() WHERE id = $1`, c.id)
	if err != nil {
		return errors.Wrapf(err, "error unstopping contact")
	}
	c.status = ContactStatusActive
	return nil
}

// UpdateLastSeenOn updates last seen on (and modified on)
func (c *Contact) UpdateLastSeenOn(ctx context.Context, db Queryer, lastSeenOn time.Time) error {
	c.lastSeenOn = &lastSeenOn
	return UpdateContactLastSeenOn(ctx, db, c.id, lastSeenOn)
}

// UpdatePreferredURN updates the URNs for the contact (if needbe) to have the passed in URN as top priority
// with the passed in channel as the preferred channel
func (c *Contact) UpdatePreferredURN(ctx context.Context, db Queryer, org *OrgAssets, urnID URNID, channel *Channel) error {
	// no urns? that's an error
	if len(c.urns) == 0 {
		return errors.Errorf("can't set preferred URN on contact with no URNs")
	}

	// is this already our top URN?
	topURNID := URNID(GetURNInt(c.urns[0], "id"))
	topChannelID := GetURNChannelID(org, c.urns[0])

	// we are already the top URN, nothing to do
	if topURNID == urnID && topChannelID != NilChannelID && channel != nil && topChannelID == channel.ID() {
		return nil
	}

	// we need to build a new list, first find our URN
	topURN := urns.NilURN
	newURNs := make([]urns.URN, 0, len(c.urns))

	priority := topURNPriority - 1
	for _, urn := range c.urns {
		id := URNID(GetURNInt(urn, "id"))
		if id == urnID {
			updated, err := updateURNChannelPriority(urn, channel, topURNPriority)
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
		ContactID: ContactID(c.ID()),
		URNs:      c.urns,
	}

	// write our new state to the db
	err := UpdateContactURNs(ctx, db, org, []*ContactURNsChanged{change})
	if err != nil {
		return errors.Wrapf(err, "error updating urns for contact")
	}

	err = UpdateContactModifiedOn(ctx, db, []ContactID{c.ID()})
	if err != nil {
		return errors.Wrapf(err, "error updating modified on on contact")
	}

	return nil
}

// FlowContact converts our mailroom contact into a flow contact for use in the engine
func (c *Contact) FlowContact(org *OrgAssets) (*flows.Contact, error) {
	// convert our groups to a list of references
	groups := make([]*assets.GroupReference, len(c.groups))
	for i, g := range c.groups {
		groups[i] = assets.NewGroupReference(g.UUID(), g.Name())
	}

	// create our flow contact
	contact, err := flows.NewContact(
		org.SessionAssets(),
		c.uuid,
		flows.ContactID(c.id),
		c.name,
		c.language,
		contactToFlowStatus[c.Status()],
		org.Env().Timezone(),
		c.createdOn,
		c.lastSeenOn,
		c.urns,
		groups,
		c.fields,
		assets.IgnoreMissing,
	)
	if err != nil {
		return nil, errors.Wrapf(err, "error creating flow contact")
	}

	return contact, nil
}

// LoadContact loads a contact from the passed in id
func LoadContact(ctx context.Context, db Queryer, org *OrgAssets, id ContactID) (*Contact, error) {
	contacts, err := LoadContacts(ctx, db, org, []ContactID{id})
	if err != nil {
		return nil, err
	}
	if len(contacts) == 0 {
		return nil, nil
	}
	return contacts[0], nil
}

// LoadContacts loads a set of contacts for the passed in ids. Note that the order of the returned contacts
// won't necessarily match the order of the ids.
func LoadContacts(ctx context.Context, db Queryer, org *OrgAssets, ids []ContactID) ([]*Contact, error) {
	start := time.Now()

	rows, err := db.QueryxContext(ctx, selectContactSQL, pq.Array(ids), org.OrgID())
	if err != nil {
		return nil, errors.Wrap(err, "error selecting contacts")
	}
	defer rows.Close()

	contacts := make([]*Contact, 0, len(ids))
	for rows.Next() {
		e := &contactEnvelope{}
		err := dbutil.ReadJSONRow(rows, e)
		if err != nil {
			return nil, errors.Wrap(err, "error scanning contact json")
		}

		contact := &Contact{
			id:         ContactID(e.ID),
			uuid:       e.UUID,
			name:       e.Name,
			language:   e.Language,
			status:     e.Status,
			createdOn:  e.CreatedOn,
			modifiedOn: e.ModifiedOn,
			lastSeenOn: e.LastSeenOn,
		}

		// load our real groups
		groups := make([]*Group, 0, len(e.GroupIDs))
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
			urn, err := u.AsURN(org)
			if err != nil {
				logrus.WithField("urn", u).WithField("org_id", org.OrgID()).WithField("contact_id", contact.id).Warn("invalid URN, ignoring")
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

// LoadContactsByUUID loads a set of contacts for the passed in UUIDs
func LoadContactsByUUID(ctx context.Context, db Queryer, oa *OrgAssets, uuids []flows.ContactUUID) ([]*Contact, error) {
	ids, err := getContactIDsFromUUIDs(ctx, db, oa.OrgID(), uuids)
	if err != nil {
		return nil, err
	}
	return LoadContacts(ctx, db, oa, ids)
}

// GetNewestContactModifiedOn returns the newest modified_on for a contact in the passed in org
func GetNewestContactModifiedOn(ctx context.Context, db Queryer, org *OrgAssets) (*time.Time, error) {
	rows, err := db.QueryxContext(ctx, "SELECT modified_on FROM contacts_contact WHERE org_id = $1 ORDER BY modified_on DESC LIMIT 1", org.OrgID())
	if err != nil && err != sql.ErrNoRows {
		return nil, errors.Wrapf(err, "error selecting most recently changed contact for org: %d", org.OrgID())
	}
	defer rows.Close()
	if err != sql.ErrNoRows {
		rows.Next()
		var newest time.Time
		err = rows.Scan(&newest)
		if err != nil {
			return nil, errors.Wrapf(err, "error scanning most recent contact modified_on for org: %d", org.OrgID())
		}

		return &newest, nil
	}

	return nil, nil
}

// GetContactIDsFromReferences gets the contact ids for the given org and set of references. Note that the order of the returned contacts
// won't necessarily match the order of the references.
func GetContactIDsFromReferences(ctx context.Context, db Queryer, orgID OrgID, refs []*flows.ContactReference) ([]ContactID, error) {
	// build our list of UUIDs
	uuids := make([]flows.ContactUUID, len(refs))
	for i := range refs {
		uuids[i] = refs[i].UUID
	}

	return getContactIDsFromUUIDs(ctx, db, orgID, uuids)
}

// gets the contact IDs for the passed in org and set of UUIDs
func getContactIDsFromUUIDs(ctx context.Context, db Queryer, orgID OrgID, uuids []flows.ContactUUID) ([]ContactID, error) {
	ids, err := queryContactIDs(ctx, db, `SELECT id FROM contacts_contact WHERE org_id = $1 AND uuid = ANY($2) AND is_active = TRUE`, orgID, pq.Array(uuids))
	if err != nil {
		return nil, errors.Wrapf(err, "error selecting contact ids by UUID")
	}
	return ids, nil
}

// utility to query contact IDs
func queryContactIDs(ctx context.Context, db Queryer, query string, args ...interface{}) ([]ContactID, error) {
	ids := make([]ContactID, 0, 10)
	rows, err := db.QueryxContext(ctx, query, args...)
	if err != nil && err != sql.ErrNoRows {
		return nil, errors.Wrapf(err, "error querying contact ids")
	}
	defer rows.Close()

	var id ContactID
	for rows.Next() {
		err := rows.Scan(&id)
		if err != nil {
			return nil, errors.Wrapf(err, "error scanning contact id")
		}
		ids = append(ids, id)
	}
	return ids, nil
}

// fieldValueEnvelope is our utility struct for the value of a field
type fieldValueEnvelope struct {
	Text     types.XText       `json:"text"`
	Datetime *types.XDateTime  `json:"datetime,omitempty"`
	Number   *types.XNumber    `json:"number,omitempty"`
	State    envs.LocationPath `json:"state,omitempty"`
	District envs.LocationPath `json:"district,omitempty"`
	Ward     envs.LocationPath `json:"ward,omitempty"`
}

type ContactURN struct {
	ID        URNID     `json:"id"          db:"id"`
	Priority  int       `json:"priority"    db:"priority"`
	Scheme    string    `json:"scheme"      db:"scheme"`
	Path      string    `json:"path"        db:"path"`
	Display   string    `json:"display"     db:"display"`
	Auth      string    `json:"auth"        db:"auth"`
	ChannelID ChannelID `json:"channel_id"  db:"channel_id"`
}

// AsURN returns a full URN representation including the query parameters needed by goflow and mailroom
func (u *ContactURN) AsURN(org *OrgAssets) (urns.URN, error) {
	// load any channel if present
	var channel *Channel
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
		return urns.NilURN, errors.Wrapf(err, "invalid URN %s:%s", u.Scheme, u.Path)
	}

	return urn, nil
}

// contactEnvelope is our JSON structure for a contact as read from the database
type contactEnvelope struct {
	ID         ContactID                                `json:"id"`
	UUID       flows.ContactUUID                        `json:"uuid"`
	Name       string                                   `json:"name"`
	Language   envs.Language                            `json:"language"`
	Status     ContactStatus                            `json:"status"`
	Fields     map[assets.FieldUUID]*fieldValueEnvelope `json:"fields"`
	GroupIDs   []GroupID                                `json:"group_ids"`
	URNs       []ContactURN                             `json:"urns"`
	CreatedOn  time.Time                                `json:"created_on"`
	ModifiedOn time.Time                                `json:"modified_on"`
	LastSeenOn *time.Time                               `json:"last_seen_on"`
}

const selectContactSQL = `
SELECT ROW_TO_JSON(r) FROM (SELECT
	id,
	org_id,
	uuid,
	name,
	language,
	status,
	is_active,
	created_on,
	modified_on,
	last_seen_on,
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
	is_active = TRUE AND
	c.org_id = $2
) r;
`

// CreateContact creates a new contact for the passed in org with the passed in URNs
func CreateContact(ctx context.Context, db QueryerWithTx, oa *OrgAssets, userID UserID, name string, language envs.Language, urnz []urns.URN) (*Contact, *flows.Contact, error) {
	// ensure all URNs are normalized
	for i, urn := range urnz {
		urnz[i] = urn.Normalize(string(oa.Env().DefaultCountry()))
	}

	// find current owners of these URNs
	owners, err := contactIDsFromURNs(ctx, db, oa.OrgID(), urnz)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "error looking up contacts for URNs")
	}

	if len(uniqueContactIDs(owners)) > 0 {
		return nil, nil, errors.New("URNs in use by other contacts")
	}

	contactID, err := tryInsertContactAndURNs(ctx, db, oa.OrgID(), userID, name, language, urnz, NilChannelID)
	if err != nil {
		// always possible that another thread created a contact with these URNs after we checked above
		if dbutil.IsUniqueViolation(err) {
			return nil, nil, errors.New("URNs in use by other contacts")
		}
		return nil, nil, err
	}

	// load a full contact so that we can calculate dynamic groups
	contacts, err := LoadContacts(ctx, db, oa, []ContactID{contactID})
	if err != nil {
		return nil, nil, errors.Wrapf(err, "error loading new contact")
	}
	contact := contacts[0]

	flowContact, err := contact.FlowContact(oa)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "error creating flow contact")
	}

	err = CalculateDynamicGroups(ctx, db, oa, flowContact)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "error calculating dynamic groups")
	}

	return contact, flowContact, nil
}

// GetOrCreateContact fetches or creates a new contact for the passed in org with the passed in URNs.
//
// * If none of the URNs exist, it creates a new contact with those URNs.
// * If URNs exist but are orphaned it creates a new contact and assigns those URNs to them.
// * If URNs exists and belongs to a single contact it returns that contact (other URNs are not assigned to the contact).
// * If URNs exists and belongs to multiple contacts it will return an error.
//
func GetOrCreateContact(ctx context.Context, db QueryerWithTx, oa *OrgAssets, urnz []urns.URN, channelID ChannelID) (*Contact, *flows.Contact, bool, error) {
	// ensure all URNs are normalized
	for i, urn := range urnz {
		urnz[i] = urn.Normalize(string(oa.Env().DefaultCountry()))
	}

	contactID, created, err := getOrCreateContact(ctx, db, oa.OrgID(), urnz, channelID)
	if err != nil {
		return nil, nil, false, err
	}

	// load a full contact so that we can calculate dynamic groups
	contacts, err := LoadContacts(ctx, db, oa, []ContactID{contactID})
	if err != nil {
		return nil, nil, false, errors.Wrapf(err, "error loading new contact")
	}
	contact := contacts[0]

	flowContact, err := contact.FlowContact(oa)
	if err != nil {
		return nil, nil, false, errors.Wrapf(err, "error creating flow contact")
	}

	// calculate dynamic groups if contact was created
	if created {
		err := CalculateDynamicGroups(ctx, db, oa, flowContact)
		if err != nil {
			return nil, nil, false, errors.Wrapf(err, "error calculating dynamic groups")
		}
	}

	return contact, flowContact, created, nil
}

// GetOrCreateContactIDsFromURNs will fetch or create the contacts for the passed in URNs, returning a map the same length as
// the passed in URNs with the ids of the contacts.
func GetOrCreateContactIDsFromURNs(ctx context.Context, db QueryerWithTx, oa *OrgAssets, urnz []urns.URN) (map[urns.URN]ContactID, error) {
	// ensure all URNs are normalized
	for i, urn := range urnz {
		urnz[i] = urn.Normalize(string(oa.Env().DefaultCountry()))
	}

	// find current owners of these URNs
	owners, err := contactIDsFromURNs(ctx, db, oa.OrgID(), urnz)
	if err != nil {
		return nil, errors.Wrapf(err, "error looking up contacts for URNs")
	}

	// create any contacts that are missing
	for urn, contactID := range owners {
		if contactID == NilContactID {
			contact, _, _, err := GetOrCreateContact(ctx, db, oa, []urns.URN{urn}, NilChannelID)
			if err != nil {
				return nil, errors.Wrapf(err, "error creating contact")
			}
			owners[urn] = contact.ID()
		}
	}
	return owners, nil
}

// looks up the contacts who own the given urns (which should be normalized by the caller) and returns that information as a map
func contactIDsFromURNs(ctx context.Context, db Queryer, orgID OrgID, urnz []urns.URN) (map[urns.URN]ContactID, error) {
	identityToOriginal := make(map[urns.URN]urns.URN, len(urnz))
	identities := make([]urns.URN, len(urnz))
	owners := make(map[urns.URN]ContactID, len(urnz))

	for i, urn := range urnz {
		identity := urn.Identity()
		identityToOriginal[identity] = urn
		identities[i] = identity
		owners[urn] = NilContactID
	}

	rows, err := db.QueryxContext(ctx, `SELECT contact_id, identity FROM contacts_contacturn WHERE org_id = $1 AND identity = ANY($2)`, orgID, pq.Array(identities))
	if err != nil && err != sql.ErrNoRows {
		return nil, errors.Wrapf(err, "error querying contact URNs")
	}
	defer rows.Close()

	for rows.Next() {
		var urn urns.URN
		var id ContactID
		if err := rows.Scan(&id, &urn); err != nil {
			return nil, errors.Wrapf(err, "error scanning URN result")
		}
		owners[identityToOriginal[urn]] = id
	}

	return owners, nil
}

func getOrCreateContact(ctx context.Context, db QueryerWithTx, orgID OrgID, urnz []urns.URN, channelID ChannelID) (ContactID, bool, error) {
	// find current owners of these URNs
	owners, err := contactIDsFromURNs(ctx, db, orgID, urnz)
	if err != nil {
		return NilContactID, false, errors.Wrapf(err, "error looking up contacts for URNs")
	}

	uniqueOwners := uniqueContactIDs(owners)
	if len(uniqueOwners) > 1 {
		return NilContactID, false, errors.New("error because URNs belong to different contacts")
	} else if len(uniqueOwners) == 1 {
		return uniqueOwners[0], false, nil
	}

	contactID, err := tryInsertContactAndURNs(ctx, db, orgID, UserID(1), "", envs.NilLanguage, urnz, channelID)
	if err == nil {
		return contactID, true, nil
	}

	if dbutil.IsUniqueViolation(err) {
		// another thread must have created contacts with these URNs in the time between us looking them up and trying to
		// create them ourselves, so let's try to fetch that contact
		owners, err := contactIDsFromURNs(ctx, db, orgID, urnz)
		if err != nil {
			return NilContactID, false, errors.Wrapf(err, "error looking up contacts for URNs")
		}

		uniqueOwners := uniqueContactIDs(owners)
		if len(uniqueOwners) > 1 {
			return NilContactID, false, errors.New("error because URNs belong to different contacts")
		} else if len(uniqueOwners) == 1 {
			return uniqueOwners[0], false, nil
		} else {
			return NilContactID, false, errors.New("lookup of URNs after failed insert returned zero contacts")
		}
	}

	return NilContactID, false, err
}

// utility to extract non-nil unique contact IDs from the given URN map
func uniqueContactIDs(urnMap map[urns.URN]ContactID) []ContactID {
	unique := make([]ContactID, 0, len(urnMap))
	seen := make(map[ContactID]bool)
	for _, id := range urnMap {
		if id != NilContactID && !seen[id] {
			unique = append(unique, id)
			seen[id] = true
		}
	}
	return unique
}

// Tries to create a new contact for the passed in org with the passed in URNs. Returned error can be tested with `dbutil.IsUniqueViolation` to
// determine if problem was one or more of the URNs already exist and are assigned to other contacts.
func tryInsertContactAndURNs(ctx context.Context, db QueryerWithTx, orgID OrgID, userID UserID, name string, language envs.Language, urnz []urns.URN, channelID ChannelID) (ContactID, error) {
	// check the URNs are valid
	for _, urn := range urnz {
		if err := urn.Validate(); err != nil {
			return NilContactID, errors.Wrapf(err, "can't insert invalid URN '%s'", urn)
		}
	}

	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		return NilContactID, errors.Wrapf(err, "error beginning transaction")
	}

	contactID, err := insertContactAndURNs(ctx, tx, orgID, userID, name, language, urnz, channelID)
	if err != nil {
		tx.Rollback()
		return NilContactID, err
	}

	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		return NilContactID, errors.Wrapf(err, "error committing transaction")
	}

	return contactID, nil
}

func insertContactAndURNs(ctx context.Context, db Queryer, orgID OrgID, userID UserID, name string, language envs.Language, urnz []urns.URN, channelID ChannelID) (ContactID, error) {
	if userID == NilUserID {
		userID = UserID(1)
	}

	// first insert our contact
	var contactID ContactID
	err := db.GetContext(ctx, &contactID,
		`INSERT INTO contacts_contact (org_id, is_active, status, uuid, name, language, created_on, modified_on, created_by_id, modified_by_id) 
		VALUES($1, TRUE, 'A', $2, $3, $4, $5, $5, $6, $6)
		RETURNING id`,
		orgID, uuids.New(), null.String(name), null.String(string(language)), dates.Now(), userID,
	)
	if err != nil {
		return NilContactID, errors.Wrapf(err, "error inserting new contact")
	}

	priority := topURNPriority

	for _, urn := range urnz {
		// look for a URN with this identity that already exists but doesn't have a contact so could be attached
		var orphanURNID URNID
		err = db.GetContext(ctx, &orphanURNID, `SELECT id FROM contacts_contacturn WHERE org_id = $1 AND identity = $2 AND contact_id IS NULL`, orgID, urn.Identity())
		if err != nil && err != sql.ErrNoRows {
			return NilContactID, err
		}
		if orphanURNID != NilURNID {
			_, err := db.ExecContext(ctx, `UPDATE contacts_contacturn SET contact_id = $2, priority = $3 WHERE id = $1`, orphanURNID, contactID, priority)
			if err != nil {
				return NilContactID, errors.Wrapf(err, "error attaching existing URN to new contact")
			}
		} else {
			_, err := db.ExecContext(ctx,
				`INSERT INTO contacts_contacturn(org_id, identity, path, scheme, display, auth, priority, channel_id, contact_id)
			     VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
				orgID, urn.Identity(), urn.Path(), urn.Scheme(), urn.Display(), GetURNAuth(urn), priority, channelID, contactID,
			)
			if err != nil {
				return NilContactID, err
			}
		}

		priority--
	}

	return contactID, nil
}

// URNForURN will return a URN for the passed in URN including all the special query parameters
// set that goflow and mailroom depend on.
func URNForURN(ctx context.Context, db Queryer, org *OrgAssets, u urns.URN) (urns.URN, error) {
	urn := &ContactURN{}
	rows, err := db.QueryxContext(ctx,
		`SELECT row_to_json(r) FROM (SELECT id, scheme, path, display, auth, channel_id, priority FROM contacts_contacturn WHERE identity = $1 AND org_id = $2) r;`,
		u.Identity(), org.OrgID(),
	)
	if err != nil {
		return urns.NilURN, errors.Errorf("error selecting URN: %s", u.Identity())
	}
	defer rows.Close()

	if !rows.Next() {
		return urns.NilURN, errors.Errorf("no urn with identity: %s", u.Identity())
	}

	err = dbutil.ReadJSONRow(rows, urn)
	if err != nil {
		return urns.NilURN, errors.Wrapf(err, "error loading contact urn")
	}

	if rows.Next() {
		return urns.NilURN, errors.Wrapf(err, "more than one URN returned for identity query")
	}

	return urn.AsURN(org)
}

// GetOrCreateURN will look up a URN by identity, creating it if needbe and associating it with the contact
func GetOrCreateURN(ctx context.Context, db Queryer, org *OrgAssets, contactID ContactID, u urns.URN) (urns.URN, error) {
	// first try to get it directly
	urn, _ := URNForURN(ctx, db, org, u)

	// found it? we are done
	if urn != urns.NilURN {
		return urn, nil
	}

	// otherwise we need to insert it
	insert := &urnInsert{
		ContactID: contactID,
		Identity:  u.Identity().String(),
		Path:      u.Path(),
		Display:   null.String(u.Display()),
		Auth:      GetURNAuth(u),
		Scheme:    u.Scheme(),
		Priority:  defaultURNPriority,
		OrgID:     org.OrgID(),
	}

	_, err := db.NamedExecContext(ctx, insertContactURNsSQL, insert)
	if err != nil {
		return urns.NilURN, errors.Wrapf(err, "error inserting new urn: %s", u)
	}

	// do a lookup once more
	return URNForURN(ctx, db, org, u)
}

// URNForID will return a URN for the passed in ID including all the special query parameters
// set that goflow and mailroom depend on. Generally this URN is built when loading a contact
// but occasionally we need to load URNs one by one and this accomplishes that
func URNForID(ctx context.Context, db Queryer, org *OrgAssets, urnID URNID) (urns.URN, error) {
	urn := &ContactURN{}
	rows, err := db.QueryxContext(ctx,
		`SELECT row_to_json(r) FROM (SELECT id, scheme, path, display, auth, channel_id, priority FROM contacts_contacturn WHERE id = $1) r;`,
		urnID,
	)
	if err != nil {
		return urns.NilURN, errors.Errorf("error selecting URN ID: %d", urnID)
	}
	defer rows.Close()

	if !rows.Next() {
		return urns.NilURN, errors.Errorf("no urn with id: %d", urnID)
	}

	err = dbutil.ReadJSONRow(rows, urn)
	if err != nil {
		return urns.NilURN, errors.Wrapf(err, "error loading contact urn")
	}

	return urn.AsURN(org)
}

// CalculateDynamicGroups recalculates all the dynamic groups for the passed in contact, recalculating
// campaigns as necessary based on those group changes.
func CalculateDynamicGroups(ctx context.Context, db Queryer, org *OrgAssets, contact *flows.Contact) error {
	added, removed, errs := contact.ReevaluateQueryBasedGroups(org.Env())
	if len(errs) > 0 {
		return errors.Wrapf(errs[0], "error calculating dynamic groups")
	}

	campaigns := make(map[CampaignID]*Campaign)

	groupAdds := make([]*GroupAdd, 0, 1)
	for _, a := range added {
		group := org.GroupByUUID(a.UUID())
		if group == nil {
			return errors.Errorf("added to unknown group: %s", a.UUID())
		}
		groupAdds = append(groupAdds, &GroupAdd{
			ContactID: ContactID(contact.ID()),
			GroupID:   group.ID(),
		})

		// add in any campaigns we may qualify for
		for _, c := range org.CampaignByGroupID(group.ID()) {
			campaigns[c.ID()] = c
		}
	}
	err := AddContactsToGroups(ctx, db, groupAdds)
	if err != nil {
		return errors.Wrapf(err, "error adding contact to groups")
	}

	groupRemoves := make([]*GroupRemove, 0, 1)
	for _, r := range removed {
		group := org.GroupByUUID(r.UUID())
		if group == nil {
			return errors.Wrapf(err, "removed from an unknown group: %s", r.UUID())
		}
		groupRemoves = append(groupRemoves, &GroupRemove{
			ContactID: ContactID(contact.ID()),
			GroupID:   group.ID(),
		})
	}
	err = RemoveContactsFromGroups(ctx, db, groupRemoves)
	if err != nil {
		return errors.Wrapf(err, "error removing contact from group")
	}

	// clear any unfired campaign events for this contact
	err = DeleteUnfiredContactEvents(ctx, db, ContactID(contact.ID()))
	if err != nil {
		return errors.Wrapf(err, "error deleting unfired events for contact")
	}

	// for each campaign figure out if we need to be added to any events
	fireAdds := make([]*FireAdd, 0, 2)
	tz := org.Env().Timezone()
	now := time.Now()
	for _, c := range campaigns {
		for _, ce := range c.Events() {
			scheduled, err := ce.ScheduleForContact(tz, now, contact)
			if err != nil {
				return errors.Wrapf(err, "error calculating schedule for event: %d", ce.ID())
			}

			if scheduled != nil {
				fireAdds = append(fireAdds, &FireAdd{
					ContactID: ContactID(contact.ID()),
					EventID:   ce.ID(),
					Scheduled: *scheduled,
				})
			}
		}
	}

	// add any event adds
	err = AddEventFires(ctx, db, fireAdds)
	if err != nil {
		return errors.Wrapf(err, "unable to add new event fires for contact")
	}

	return nil
}

// StopContact stops the contact with the passed in id, removing them from all groups and setting
// their state to stopped.
func StopContact(ctx context.Context, db Queryer, orgID OrgID, contactID ContactID) error {
	// delete the contact from all groups
	_, err := db.ExecContext(ctx, deleteAllContactGroupsSQL, orgID, contactID)
	if err != nil {
		return errors.Wrapf(err, "error removing stopped contact from groups")
	}

	// remove all unfired campaign event fires
	_, err = db.ExecContext(ctx, deleteUnfiredEventsSQL, contactID)
	if err != nil {
		return errors.Wrapf(err, "error deleting unfired event fires")
	}

	// remove the contact from any triggers
	// TODO: this could leave a trigger with no contacts or groups
	_, err = db.ExecContext(ctx, deleteAllContactTriggersSQL, contactID)
	if err != nil {
		return errors.Wrapf(err, "error removing contact from triggers")
	}

	// mark as stopped
	_, err = db.ExecContext(ctx, markContactStoppedSQL, contactID)
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
	contactgroup_id = ANY(SELECT id from contacts_contactgroup WHERE org_id = $1 and group_type = 'U')
`

const deleteAllContactTriggersSQL = `
DELETE FROM
	triggers_trigger_contacts
WHERE
	contact_id = $1
`

const deleteUnfiredEventsSQL = `
DELETE FROM
	campaigns_eventfire
WHERE
	contact_id = $1 AND
	fired IS NULL
`

const markContactStoppedSQL = `
UPDATE
	contacts_contact
SET
	status = 'S',
	modified_on = NOW()
WHERE 
	id = $1
`

func GetURNInt(urn urns.URN, key string) int {
	values, err := urn.Query()
	if err != nil {
		return 0
	}

	value, _ := strconv.Atoi(values.Get(key))
	return value
}

func GetURNAuth(urn urns.URN) null.String {
	values, err := urn.Query()
	if err != nil {
		return null.NullString
	}

	value := values.Get("auth")
	if value == "" {
		return null.NullString
	}
	return null.String(value)
}

func GetURNChannelID(org *OrgAssets, urn urns.URN) ChannelID {
	values, err := urn.Query()
	if err != nil {
		return NilChannelID
	}

	channelUUID := values.Get("channel")
	if channelUUID == "" {
		return NilChannelID
	}

	channel := org.ChannelByUUID(assets.ChannelUUID(channelUUID))
	if channel != nil {
		return channel.ID()
	}
	return NilChannelID
}

func GetURNID(urn urns.URN) URNID {
	values, err := urn.Query()
	if err != nil {
		return NilURNID
	}

	urnStr := values.Get("id")
	urnID, err := strconv.Atoi(urnStr)
	if err != nil {
		return NilURNID
	}

	return URNID(urnID)
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
func UpdateContactModifiedOn(ctx context.Context, db Queryer, contactIDs []ContactID) error {
	_, err := db.ExecContext(ctx, `UPDATE contacts_contact SET modified_on = NOW() WHERE id = ANY($1)`, pq.Array(contactIDs))
	return err
}

// UpdateContactLastSeenOn updates last seen on (and modified on) on the passed in contact
func UpdateContactLastSeenOn(ctx context.Context, db Queryer, contactID ContactID, lastSeenOn time.Time) error {
	_, err := db.ExecContext(ctx, `UPDATE contacts_contact SET last_seen_on = $2, modified_on = NOW() WHERE id = $1`, contactID, lastSeenOn)
	return err
}

// UpdateContactURNs updates the contact urns in our database to match the passed in changes
func UpdateContactURNs(ctx context.Context, db Queryer, org *OrgAssets, changes []*ContactURNsChanged) error {
	// keep track of all our inserts
	inserts := make([]interface{}, 0, len(changes))

	// and updates to URNs
	updates := make([]interface{}, 0, len(changes))

	contactIDs := make([]ContactID, 0)
	updatedURNIDs := make([]URNID, 0)

	// identities we are inserting
	identities := make([]string, 0, 1)

	// for each of our changes (one per contact)
	for _, change := range changes {
		contactIDs = append(contactIDs, change.ContactID)

		// priority for each contact starts at 1000
		priority := topURNPriority

		// for each of our urns
		for _, urn := range change.URNs {
			// figure out if we have a channel
			channelID := GetURNChannelID(org, urn)

			// do we have an id?
			urnID := URNID(GetURNInt(urn, "id"))

			if urnID > 0 {
				// if so, this is a URN update
				updates = append(updates, &urnUpdate{
					URNID:     urnID,
					ChannelID: channelID,
					Priority:  priority,
				})

				updatedURNIDs = append(updatedURNIDs, urnID)
			} else {
				// new URN, add it instead
				inserts = append(inserts, &urnInsert{
					ContactID: change.ContactID,
					Identity:  urn.Identity().String(),
					Path:      urn.Path(),
					Display:   null.String(urn.Display()),
					Auth:      GetURNAuth(urn),
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
	err := BulkQuery(ctx, "updating contact urns", db, updateContactURNsSQL, updates)
	if err != nil {
		return errors.Wrapf(err, "error updating urns")
	}

	// then detach any URNs that weren't updated (the ones we're not keeping)
	_, err = db.ExecContext(
		ctx,
		`UPDATE contacts_contacturn SET contact_id = NULL WHERE contact_id = ANY($1) AND id != ALL($2)`,
		pq.Array(contactIDs),
		pq.Array(updatedURNIDs),
	)
	if err != nil {
		return errors.Wrapf(err, "error detaching urns")
	}

	if len(inserts) > 0 {
		// find the unique ids of the contacts that may be affected by our URN inserts
		orphanedIDs, err := queryContactIDs(ctx, db, `SELECT contact_id FROM contacts_contacturn WHERE identity = ANY($1) AND org_id = $2 AND contact_id IS NOT NULL`, pq.Array(identities), org.OrgID())
		if err != nil {
			return errors.Wrapf(err, "error finding contacts for URNs")
		}

		// then insert new urns, we do these one by one since we have to deal with conflicts
		for _, insert := range inserts {
			_, err := db.NamedExecContext(ctx, insertContactURNsSQL, insert)
			if err != nil {
				return errors.Wrapf(err, "error inserting new urns")
			}
		}

		// finally mark all the orphaned contacts as modified
		if len(orphanedIDs) > 0 {
			err := UpdateContactModifiedOn(ctx, db, orphanedIDs)
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
	URNID     URNID     `db:"id"`
	ChannelID ChannelID `db:"channel_id"`
	Priority  int       `db:"priority"`
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
	ContactID ContactID   `db:"contact_id"`
	Identity  string      `db:"identity"`
	Path      string      `db:"path"`
	Display   null.String `db:"display"`
	Auth      null.String `db:"auth"`
	Scheme    string      `db:"scheme"`
	Priority  int         `db:"priority"`
	OrgID     OrgID       `db:"org_id"`
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
	ContactID ContactID
	OrgID     OrgID
	URNs      []urns.URN
}

// MarshalJSON marshals into JSON. 0 values will become null
func (i URNID) MarshalJSON() ([]byte, error) {
	return null.Int(i).MarshalJSON()
}

// UnmarshalJSON unmarshals from JSON. null values become 0
func (i *URNID) UnmarshalJSON(b []byte) error {
	return null.UnmarshalInt(b, (*null.Int)(i))
}

// Value returns the db value, null is returned for 0
func (i URNID) Value() (driver.Value, error) {
	return null.Int(i).Value()
}

// Scan scans from the db value. null values become 0
func (i *URNID) Scan(value interface{}) error {
	return null.ScanInt(value, (*null.Int)(i))
}

// MarshalJSON marshals into JSON. 0 values will become null
func (i ContactID) MarshalJSON() ([]byte, error) {
	return null.Int(i).MarshalJSON()
}

// UnmarshalJSON unmarshals from JSON. null values become 0
func (i *ContactID) UnmarshalJSON(b []byte) error {
	return null.UnmarshalInt(b, (*null.Int)(i))
}

// Value returns the db value, null is returned for 0
func (i ContactID) Value() (driver.Value, error) {
	return null.Int(i).Value()
}

// Scan scans from the db value. null values become 0
func (i *ContactID) Scan(value interface{}) error {
	return null.ScanInt(value, (*null.Int)(i))
}

// ContactLock returns the lock key for a particular contact, used with locker
func ContactLock(orgID OrgID, contactID ContactID) string {
	return fmt.Sprintf("c:%d:%d", orgID, contactID)
}

// UpdateContactModifiedBy updates modified by the passed user id on the passed in contacts
func UpdateContactModifiedBy(ctx context.Context, db Queryer, contactIDs []ContactID, userID UserID) error {
	if userID == NilUserID || len(contactIDs) == 0 {
		return nil
	}
	_, err := db.ExecContext(ctx, `UPDATE contacts_contact SET modified_on = NOW(), modified_by_id = $2 WHERE id = ANY($1)`, pq.Array(contactIDs), userID)
	return err
}

// ContactStatusChange struct used for our contact status change
type ContactStatusChange struct {
	ContactID ContactID
	Status    flows.ContactStatus
}

type contactStatusUpdate struct {
	ContactID ContactID     `db:"id"`
	Status    ContactStatus `db:"status"`
}

// UpdateContactStatus updates the contacts status as the passed changes
func UpdateContactStatus(ctx context.Context, db Queryer, changes []*ContactStatusChange) error {

	archiveTriggersForContactIDs := make([]ContactID, 0, len(changes))
	statusUpdates := make([]interface{}, 0, len(changes))

	for _, ch := range changes {
		blocked := ch.Status == flows.ContactStatusBlocked
		stopped := ch.Status == flows.ContactStatusStopped
		status := contactToModelStatus[ch.Status]

		if blocked || stopped {
			archiveTriggersForContactIDs = append(archiveTriggersForContactIDs, ch.ContactID)
		}

		statusUpdates = append(
			statusUpdates,
			&contactStatusUpdate{
				ContactID: ch.ContactID,
				Status:    status,
			},
		)

	}

	err := ArchiveContactTriggers(ctx, db, archiveTriggersForContactIDs)
	if err != nil {
		return errors.Wrapf(err, "error archiving triggers for blocked or stopped contacts")
	}

	// do our status update
	err = BulkQuery(ctx, "updating contact statuses", db, updateContactStatusSQL, statusUpdates)
	if err != nil {
		return errors.Wrapf(err, "error updating contact statuses")
	}

	return err
}

const updateContactStatusSQL = `
	UPDATE
		contacts_contact c
	SET
		status = r.status,
		modified_on = NOW()
	FROM (
		VALUES(:id, :status)
	) AS
		r(id, status)
	WHERE
		c.id = r.id::int
`
