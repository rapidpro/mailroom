package models

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"time"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/contactql"
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/goflow/excellent/types"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/goflow/utils/uuids"
	"github.com/nyaruka/mailroom/search"
	"github.com/nyaruka/null"
	"github.com/olivere/elastic"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// URNID is our type for urn ids, which can be null
type URNID null.Int

// ContactID is our type for contact ids, which can be null
type ContactID null.Int

const (
	topURNPriority     = 1000
	defaultURNPriority = 0

	NilURNID     = URNID(0)
	NilContactID = ContactID(0)
)

// LoadContacts loads a set of contacts for the passed in ids
func LoadContacts(ctx context.Context, db Queryer, org *OrgAssets, ids []ContactID) ([]*Contact, error) {
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
			id:         ContactID(e.ID),
			uuid:       e.UUID,
			name:       e.Name,
			language:   e.Language,
			isStopped:  e.IsStopped,
			isBlocked:  e.IsBlocked,
			modifiedOn: e.ModifiedOn,
			createdOn:  e.CreatedOn,
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

// ContactIDsFromReferences queries the contacts for the passed in org, returning the contact ids for the references
func ContactIDsFromReferences(ctx context.Context, tx Queryer, org *OrgAssets, refs []*flows.ContactReference) ([]ContactID, error) {
	// build our list of UUIDs
	uuids := make([]interface{}, len(refs))
	for i := range refs {
		uuids[i] = refs[i].UUID
	}

	ids := make([]ContactID, 0, len(refs))
	rows, err := tx.QueryxContext(ctx,
		`SELECT id FROM contacts_contact WHERE org_id = $1 AND uuid = ANY($2) AND is_active = TRUE`,
		org.OrgID(), pq.Array(uuids),
	)
	if err != nil {
		return nil, errors.Wrapf(err, "error selecting contact ids by uuid")
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

// BuildFieldResolver builds a field resolver function for the passed in Org
func BuildFieldResolver(org *OrgAssets) contactql.FieldResolverFunc {
	return func(key string) assets.Field {
		f := org.FieldByKey(key)
		if f == nil {
			return nil
		}
		return f
	}
}

// BuildElasticQuery turns the passed in contact ql query into an elastic query
func BuildElasticQuery(org *OrgAssets, resolver contactql.FieldResolverFunc, query *contactql.ContactQuery) (elastic.Query, error) {
	// filter by org and active contacts
	eq := elastic.NewBoolQuery().Must(
		elastic.NewTermQuery("org_id", org.OrgID()),
		elastic.NewTermQuery("is_active", true),
	)

	// and by our query if present
	if query != nil {
		q, err := search.ToElasticQuery(org.Env(), resolver, query)
		if err != nil {
			return nil, errors.Wrapf(err, "error converting contactql to elastic query: %s", query)
		}

		eq = eq.Must(q)
	}

	return eq, nil
}

// ContactIDsForQueryPage returns the ids of the contacts for the passed in query page
func ContactIDsForQueryPage(ctx context.Context, client *elastic.Client, org *OrgAssets, group assets.GroupUUID, query string, sort string, offset int, pageSize int) (*contactql.ContactQuery, []ContactID, int64, error) {
	start := time.Now()
	var parsed *contactql.ContactQuery
	var err error

	if client == nil {
		return nil, nil, 0, errors.Errorf("no elastic client available, check your configuration")
	}

	resolver := BuildFieldResolver(org)

	if query != "" {
		parsed, err = search.ParseQuery(org.Env(), resolver, query)
		if err != nil {
			return nil, nil, 0, errors.Wrapf(err, "error parsing query: %s", query)
		}
	}

	eq, err := BuildElasticQuery(org, resolver, parsed)
	if err != nil {
		return nil, nil, 0, errors.Wrapf(err, "error parsing query: %s", query)
	}

	fieldSort, err := search.ToElasticFieldSort(resolver, sort)
	if err != nil {
		return nil, nil, 0, errors.Wrapf(err, "error parsing sort")
	}

	// filter by our base group
	eq = elastic.NewBoolQuery().Must(
		eq,
		elastic.NewTermQuery("groups", group),
	)

	s := client.Search("contacts").Routing(strconv.FormatInt(int64(org.OrgID()), 10))
	s = s.Size(pageSize).From(offset).Query(eq).SortBy(fieldSort).FetchSource(false)

	results, err := s.Do(ctx)
	if err != nil {
		return nil, nil, 0, errors.Wrapf(err, "error performing query")
	}

	ids := make([]ContactID, 0, pageSize)
	for _, hit := range results.Hits.Hits {
		id, err := strconv.Atoi(hit.Id)
		if err != nil {
			return nil, nil, 0, errors.Wrapf(err, "unexpected non-integer contact id: %s for search: %s", hit.Id, query)
		}
		ids = append(ids, ContactID(id))
	}

	logrus.WithFields(logrus.Fields{
		"org_id":      org.OrgID(),
		"parsed":      parsed,
		"group_uuid":  group,
		"query":       query,
		"elapsed":     time.Since(start),
		"page_count":  len(ids),
		"total_count": results.Hits.TotalHits,
	}).Debug("paged contact query complete")

	return parsed, ids, results.Hits.TotalHits, nil
}

// ContactIDsForQuery returns the ids of all the contacts that match the passed in query
func ContactIDsForQuery(ctx context.Context, client *elastic.Client, org *OrgAssets, query string) ([]ContactID, error) {
	start := time.Now()

	if client == nil {
		return nil, errors.Errorf("no elastic client available, check your configuration")
	}

	// turn into elastic query
	resolver := BuildFieldResolver(org)
	parsed, err := search.ParseQuery(org.Env(), resolver, query)
	if err != nil {
		return nil, errors.Wrapf(err, "error parsing query: %s", query)
	}

	eq, err := BuildElasticQuery(org, resolver, parsed)
	if err != nil {
		return nil, errors.Wrapf(err, "error converting contactql to elastic query: %s", query)
	}

	// only include unblocked and unstopped contacts
	eq = elastic.NewBoolQuery().Must(
		eq,
		elastic.NewTermQuery("is_blocked", false),
		elastic.NewTermQuery("is_stopped", false),
	)

	ids := make([]ContactID, 0, 100)

	// iterate across our results, building up our contact ids
	scroll := client.Scroll("contacts").Routing(strconv.FormatInt(int64(org.OrgID()), 10))
	scroll = scroll.KeepAlive("15m").Size(10000).Query(eq).FetchSource(false)
	for {
		results, err := scroll.Do(ctx)
		if err == io.EOF {
			logrus.WithFields(logrus.Fields{
				"org_id":      org.OrgID(),
				"query":       query,
				"elapsed":     time.Since(start),
				"match_count": len(ids),
			}).Debug("contact query complete")

			return ids, nil
		}
		if err != nil {
			return nil, errors.Wrapf(err, "error scrolling through results for search: %s", query)
		}

		for _, hit := range results.Hits.Hits {
			id, err := strconv.Atoi(hit.Id)
			if err != nil {
				return nil, errors.Wrapf(err, "unexpected non-integer contact id: %s for search: %s", hit.Id, query)
			}

			ids = append(ids, ContactID(id))
		}
	}
}

// FlowContact converts our mailroom contact into a flow contact for use in the engine
func (c *Contact) FlowContact(org *OrgAssets, session flows.SessionAssets) (*flows.Contact, error) {
	// convert our groups to a list of asset groups
	groups := make([]assets.Group, len(c.groups))
	for i, g := range c.groups {
		groups[i] = g
	}

	// create our flow contact
	contact, err := flows.NewContact(
		session,
		c.uuid,
		flows.ContactID(c.id),
		c.name,
		c.language,
		org.Env().Timezone(),
		c.createdOn,
		c.urns,
		groups,
		c.fields,
	)
	if err != nil {
		return nil, errors.Wrapf(err, "error creating flow contact")
	}

	return contact, nil
}

// URNForID returns the flow URN for the passed in URN, return NilURN if not found
func (c *Contact) URNForID(urnID URNID) urns.URN {
	for _, u := range c.urns {
		if GetURNID(u) == urnID {
			return u
		}
	}

	return urns.NilURN
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
	id         ContactID
	uuid       flows.ContactUUID
	name       string
	language   envs.Language
	isStopped  bool
	isBlocked  bool
	fields     map[string]*flows.Value
	groups     []*Group
	urns       []urns.URN
	modifiedOn time.Time
	createdOn  time.Time
}

func (c *Contact) ID() ContactID                   { return c.id }
func (c *Contact) UUID() flows.ContactUUID         { return c.uuid }
func (c *Contact) Name() string                    { return c.name }
func (c *Contact) Language() envs.Language         { return c.language }
func (c *Contact) IsStopped() bool                 { return c.isStopped }
func (c *Contact) IsBlocked() bool                 { return c.isBlocked }
func (c *Contact) Fields() map[string]*flows.Value { return c.fields }
func (c *Contact) Groups() []*Group                { return c.groups }
func (c *Contact) URNs() []urns.URN                { return c.urns }
func (c *Contact) ModifiedOn() time.Time           { return c.modifiedOn }
func (c *Contact) CreatedOn() time.Time            { return c.createdOn }

// fieldValueEnvelope is our utility struct for the value of a field
type fieldValueEnvelope struct {
	Text     types.XText        `json:"text"`
	Datetime *types.XDateTime   `json:"datetime,omitempty"`
	Number   *types.XNumber     `json:"number,omitempty"`
	State    utils.LocationPath `json:"state,omitempty"`
	District utils.LocationPath `json:"district,omitempty"`
	Ward     utils.LocationPath `json:"ward,omitempty"`
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
	IsStopped  bool                                     `json:"is_stopped"`
	IsBlocked  bool                                     `json:"is_blocked"`
	Fields     map[assets.FieldUUID]*fieldValueEnvelope `json:"fields"`
	GroupIDs   []GroupID                                `json:"group_ids"`
	URNs       []ContactURN                             `json:"urns"`
	ModifiedOn time.Time                                `json:"modified_on"`
	CreatedOn  time.Time                                `json:"created_on"`
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
	is_active = TRUE
) r;
`

// ContactIDsFromURNs will fetch or create the contacts for the passed in URNs, returning a map the same length as
// the passed in URNs with the ids of the contacts.
func ContactIDsFromURNs(ctx context.Context, db *sqlx.DB, org *OrgAssets, assets flows.SessionAssets, us []urns.URN) (map[urns.URN]ContactID, error) {
	// build a map of our urns to contact id
	urnMap := make(map[urns.URN]ContactID, len(us))

	// and another map from URN identity to the passed in URN
	urnIdentities := make(map[urns.URN]urns.URN, len(us))
	for _, u := range us {
		urnIdentities[u.Identity()] = u
	}

	// try to select our contact ids
	identities := make([]string, len(us))
	for i := range us {
		if us[i] == urns.NilURN {
			return nil, errors.Errorf("cannot look up contact without URN")
		}

		identities[i] = us[i].Identity().String()
	}

	rows, err := db.QueryxContext(ctx,
		`SELECT contact_id, identity FROM contacts_contacturn WHERE org_id = $1 AND identity = ANY($2) AND contact_id IS NOT NULL`,
		org.OrgID(), pq.Array(identities),
	)

	if err != nil {
		return nil, errors.Wrapf(err, "error querying contact urns")
	}
	defer rows.Close()

	for rows.Next() {
		var urn urns.URN
		var id ContactID

		err := rows.Scan(&id, &urn)
		if err != nil {
			return nil, errors.Wrapf(err, "error scanning urn result")
		}

		original, found := urnIdentities[urn]
		if !found {
			return nil, errors.Wrapf(err, "unable to find original URN from identity")
		}

		urnMap[original] = id
	}

	// if we didn't find some contacts
	if len(urnMap) < len(us) {
		// create the contacts that are missing
		for _, u := range us {
			if urnMap[u] == NilContactID {
				id, err := CreateContact(ctx, db, org, assets, u)
				if err != nil {
					return nil, errors.Wrapf(err, "error while creating contact")
				}

				original, found := urnIdentities[u]
				if !found {
					return nil, errors.Wrapf(err, "unable to find original URN from identity")
				}
				urnMap[original] = ContactID(id)
			}
		}
	}

	// return our map of urns to ids
	return urnMap, nil
}

// CreateContact creates a new contact for the passed in org with the passed in URNs
func CreateContact(ctx context.Context, db *sqlx.DB, org *OrgAssets, assets flows.SessionAssets, urn urns.URN) (ContactID, error) {
	// we have a URN, first try to look up the URN
	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		return NilContactID, errors.Wrapf(err, "unable to start transaction")
	}

	// first insert our contact
	var contactID ContactID
	err = tx.GetContext(ctx, &contactID,
		`INSERT INTO 
		contacts_contact
			(org_id, is_active, is_blocked, is_stopped, uuid, created_on, modified_on, created_by_id, modified_by_id, name) 
		VALUES
			($1, TRUE, FALSE, FALSE, $2, NOW(), NOW(), 1, 1, '')
		RETURNING id`,
		org.OrgID(), uuids.New(),
	)

	if err != nil {
		tx.Rollback()
		return NilContactID, errors.Wrapf(err, "error inserting new contact")
	}

	// handler for when we insert the URN or commit, we try to look the contact up instead
	handleURNError := func(err error) (ContactID, error) {
		if pqErr, ok := err.(*pq.Error); ok {
			// if this was a duplicate URN, we should be able to select this contact instead
			if pqErr.Code.Name() == "unique_violation" {
				ids, err := ContactIDsFromURNs(ctx, db, org, assets, []urns.URN{urn})
				if err != nil || len(ids) == 0 {
					return NilContactID, errors.Wrapf(err, "unable to load contact for urn: %s", urn)
				}
				return ids[urn], nil
			}
		}
		return NilContactID, errors.Wrapf(err, "error creating new contact")
	}

	// now try to insert our URN if we have one
	if urn != urns.NilURN {
		_, err := tx.Exec(
			`INSERT INTO 
			contacts_contacturn
				(org_id, identity, path, scheme, display, auth, priority, channel_id, contact_id)
			VALUES
				($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
			org.OrgID(), urn.Identity(), urn.Path(), urn.Scheme(), urn.Display(), GetURNAuth(urn), topURNPriority, nil, contactID,
		)

		if err != nil {
			tx.Rollback()
			return handleURNError(err)
		}
	}

	// load a full contact so that we can calculate dynamic groups
	contacts, err := LoadContacts(ctx, tx, org, []ContactID{contactID})
	if err != nil {
		tx.Rollback()
		return NilContactID, errors.Wrapf(err, "error loading new contact")
	}

	flowContact, err := contacts[0].FlowContact(org, assets)
	if err != nil {
		tx.Rollback()
		return NilContactID, errors.Wrapf(err, "error creating flow contact")
	}

	// now calculate dynamic groups
	err = CalculateDynamicGroups(ctx, tx, org, flowContact)
	if err != nil {
		tx.Rollback()
		return NilContactID, errors.Wrapf(err, "error calculating dynamic groups")
	}

	// try to commit
	err = tx.Commit()

	if err != nil {
		tx.Rollback()
		return handleURNError(err)
	}

	return contactID, nil
}

// URNForURN will return a URN for the passed in URN including all the special query parameters
// set that goflow and mailroom depend on.
func URNForURN(ctx context.Context, tx Queryer, org *OrgAssets, u urns.URN) (urns.URN, error) {
	urn := &ContactURN{}
	rows, err := tx.QueryxContext(ctx,
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

	err = readJSONRow(rows, urn)
	if err != nil {
		return urns.NilURN, errors.Wrapf(err, "error loading contact urn")
	}

	if rows.Next() {
		return urns.NilURN, errors.Wrapf(err, "more than one URN returned for identity query")
	}

	return urn.AsURN(org)
}

// GetOrCreateURN will look up a URN by identity, creating it if needbe and associating it with the contact
func GetOrCreateURN(ctx context.Context, tx Queryer, org *OrgAssets, contactID ContactID, u urns.URN) (urns.URN, error) {
	// first try to get it directly
	urn, _ := URNForURN(ctx, tx, org, u)

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

	_, err := tx.NamedExecContext(ctx, insertContactURNsSQL, insert)
	if err != nil {
		return urns.NilURN, errors.Wrapf(err, "error inserting new urn: %s", u)
	}

	// do a lookup once more
	return URNForURN(ctx, tx, org, u)
}

// URNForID will return a URN for the passed in ID including all the special query parameters
// set that goflow and mailroom depend on. Generally this URN is built when loading a contact
// but occasionally we need to load URNs one by one and this accomplishes that
func URNForID(ctx context.Context, tx Queryer, org *OrgAssets, urnID URNID) (urns.URN, error) {
	urn := &ContactURN{}
	rows, err := tx.QueryxContext(ctx,
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

	err = readJSONRow(rows, urn)
	if err != nil {
		return urns.NilURN, errors.Wrapf(err, "error loading contact urn")
	}

	return urn.AsURN(org)
}

// CalculateDynamicGroups recalculates all the dynamic groups for the passed in contact, recalculating
// campaigns as necessary based on those group changes.
func CalculateDynamicGroups(ctx context.Context, tx Queryer, org *OrgAssets, contact *flows.Contact) error {
	orgGroups, _ := org.Groups()
	orgFields, _ := org.Fields()
	added, removed, errs := contact.ReevaluateDynamicGroups(org.Env(), flows.NewGroupAssets(orgGroups), flows.NewFieldAssets(orgFields))
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
	err := AddContactsToGroups(ctx, tx, groupAdds)
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
	err = RemoveContactsFromGroups(ctx, tx, groupRemoves)
	if err != nil {
		return errors.Wrapf(err, "error removing contact from group")
	}

	// clear any unfired campaign events for this contact
	err = DeleteUnfiredContactEvents(ctx, tx, ContactID(contact.ID()))
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
	err = AddEventFires(ctx, tx, fireAdds)
	if err != nil {
		return errors.Wrapf(err, "unable to add new event fires for contact")
	}

	return nil
}

// StopContact stops the contact with the passed in id, removing them from all groups and setting
// their state to stopped.
func StopContact(ctx context.Context, tx Queryer, orgID OrgID, contactID ContactID) error {
	// delete the contact from all groups
	_, err := tx.ExecContext(ctx, deleteAllContactGroupsSQL, orgID, contactID)
	if err != nil {
		return errors.Wrapf(err, "error removing stopped contact from groups")
	}

	// remove all unfired campaign event fires
	_, err = tx.ExecContext(ctx, deleteUnfiredEventsSQL, contactID)
	if err != nil {
		return errors.Wrapf(err, "error deleting unfired event fires")
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
	err := UpdateContactURNs(ctx, tx, org, []*ContactURNsChanged{change})
	if err != nil {
		return errors.Wrapf(err, "error updating urns for contact")
	}

	err = UpdateContactModifiedOn(ctx, tx, []ContactID{c.ID()})
	if err != nil {
		return errors.Wrapf(err, "error updating modified on on contact")
	}

	return nil
}

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
func UpdateContactModifiedOn(ctx context.Context, tx Queryer, contactIDs []ContactID) error {
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
		priority := topURNPriority

		// for each of our urns
		for _, urn := range change.URNs {
			// figure out if we have a channel
			channelID := GetURNChannelID(org, urn)

			// do we have an id?
			urnID := GetURNInt(urn, "id")

			if urnID > 0 {
				// if so, this is a URN update
				updates = append(updates, &urnUpdate{
					URNID:     URNID(urnID),
					ChannelID: channelID,
					Priority:  priority,
				})
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

		orphanedIDs := make([]ContactID, 0, len(inserts))
		for rows.Next() {
			var contactID ContactID
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
