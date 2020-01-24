package models

import (
	"context"
	"database/sql"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/nyaruka/goflow/assets"
	"github.com/olivere/elastic"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// GroupStatus is the current status of the passed in group
type GroupStatus string

const (
	GroupStatusInitializing = GroupStatus("I")
	GroupStatusEvaluating   = GroupStatus("V")
	GroupStatusReady        = GroupStatus("R")
)

// GroupID is our type for group ids
type GroupID int

// Group is our mailroom type for contact groups
type Group struct {
	g struct {
		ID    GroupID          `json:"id"`
		UUID  assets.GroupUUID `json:"uuid"`
		Name  string           `json:"name"`
		Query string           `json:"query"`
	}
}

// ID returns the ID for this group
func (g *Group) ID() GroupID { return g.g.ID }

// UUID returns the uuid for this group
func (g *Group) UUID() assets.GroupUUID { return g.g.UUID }

// Name returns the name for this group
func (g *Group) Name() string { return g.g.Name }

// Query returns the query string (if any) for this group
func (g *Group) Query() string { return g.g.Query }

// loads the groups for the passed in org
func loadGroups(ctx context.Context, db sqlx.Queryer, orgID OrgID) ([]assets.Group, error) {
	start := time.Now()

	rows, err := db.Queryx(selectGroupsSQL, orgID)
	if err != nil {
		return nil, errors.Wrapf(err, "error querying groups for org: %d", orgID)
	}
	defer rows.Close()

	groups := make([]assets.Group, 0, 10)
	for rows.Next() {
		group := &Group{}
		err = readJSONRow(rows, &group.g)
		if err != nil {
			return nil, errors.Wrap(err, "error reading group row")
		}

		groups = append(groups, group)
	}

	logrus.WithField("elapsed", time.Since(start)).WithField("org_id", orgID).WithField("count", len(groups)).Debug("loaded groups")

	return groups, nil
}

const selectGroupsSQL = `
SELECT ROW_TO_JSON(r) FROM (SELECT
	id, 
	uuid, 
	name, 
	query
FROM 
	contacts_contactgroup 
WHERE 
	org_id = $1 AND 
	is_active = TRUE AND
	group_type = 'U'
ORDER BY 
	name ASC
) r;
`

// RemoveContactsFromGroups fires a bulk SQL query to remove all the contacts in the passed in groups
func RemoveContactsFromGroups(ctx context.Context, tx Queryer, removals []*GroupRemove) error {
	if len(removals) == 0 {
		return nil
	}

	// convert to list of interfaces
	is := make([]interface{}, len(removals))
	for i := range removals {
		is[i] = removals[i]
	}
	return BulkSQL(ctx, "removing contacts from groups", tx, removeContactsFromGroupsSQL, is)
}

// GroupRemove is our struct to track group removals
type GroupRemove struct {
	ContactID ContactID `db:"contact_id"`
	GroupID   GroupID   `db:"group_id"`
}

const removeContactsFromGroupsSQL = `
DELETE FROM
	contacts_contactgroup_contacts
WHERE 
	id
IN (
	SELECT 
		c.id 
	FROM 
		contacts_contactgroup_contacts c,
		(VALUES(:contact_id, :group_id)) AS g(contact_id, group_id)
	WHERE
		c.contact_id = g.contact_id::int AND c.contactgroup_id = g.group_id::int
);
`

// AddContactsToGroups fires a bulk SQL query to remove all the contacts in the passed in groups
func AddContactsToGroups(ctx context.Context, tx Queryer, adds []*GroupAdd) error {
	if len(adds) == 0 {
		return nil
	}

	// convert to list of interfaces
	is := make([]interface{}, len(adds))
	for i := range adds {
		is[i] = adds[i]
	}
	return BulkSQL(ctx, "adding contacts to groups", tx, addContactsToGroupsSQL, is)
}

// GroupAdd is our struct to track a final group additions
type GroupAdd struct {
	ContactID ContactID `db:"contact_id"`
	GroupID   GroupID   `db:"group_id"`
}

const addContactsToGroupsSQL = `
INSERT INTO 
	contacts_contactgroup_contacts
	(contact_id, contactgroup_id)
VALUES(:contact_id, :group_id)
ON CONFLICT
	DO NOTHING
`

// ContactIDsForGroupIDs returns the unique contacts that are in the passed in groups
func ContactIDsForGroupIDs(ctx context.Context, tx Queryer, groupIDs []GroupID) ([]ContactID, error) {
	// now add all the ids for our groups
	rows, err := tx.QueryxContext(ctx, `SELECT DISTINCT(contact_id) FROM contacts_contactgroup_contacts WHERE contactgroup_id = ANY($1)`, pq.Array(groupIDs))
	if err != nil {
		return nil, errors.Wrapf(err, "error selecting contacts for groups")
	}
	defer rows.Close()

	contactIDs := make([]ContactID, 0, 10)
	var contactID ContactID
	for rows.Next() {
		err := rows.Scan(&contactID)
		if err != nil {
			return nil, errors.Wrapf(err, "error scanning contact id")
		}
		contactIDs = append(contactIDs, contactID)
	}

	return contactIDs, nil
}

const updateGroupStatusSQL = `UPDATE contacts_contactgroup SET status = $2 WHERE id = $1`

// UpdateGroupStatus updates the group status for the passed in group
func UpdateGroupStatus(ctx context.Context, db Queryer, groupID GroupID, status GroupStatus) error {
	_, err := db.ExecContext(ctx, updateGroupStatusSQL, groupID, status)
	if err != nil {
		return errors.Wrapf(err, "error updating group status for group: %d", groupID)
	}
	return nil
}

// PopulateDynamicGroup calculates which members should be part of a group and populates the contacts
// for that group by performing the minimum number of inserts / deletes.
func PopulateDynamicGroup(ctx context.Context, db Queryer, es *elastic.Client, org *OrgAssets, groupID GroupID, query string) (int, error) {
	err := UpdateGroupStatus(ctx, db, groupID, GroupStatusEvaluating)
	if err != nil {
		return 0, errors.Wrapf(err, "error marking dynamic group as evaluating")
	}

	start := time.Now()

	// we have a bit of a race with the indexer process.. we want to make sure that any contacts that changed
	// before this group was updated but after the last index are included, so if a contact was modified
	// more recently than 10 seconds ago, we wait that long before starting in populating our group
	rows, err := db.QueryxContext(ctx, "SELECT modified_on FROM contacts_contact WHERE org_id = $1 ORDER BY modified_on DESC LIMIT 1", org.OrgID())
	if err != nil && err != sql.ErrNoRows {
		return 0, errors.Wrapf(err, "error selecting most recently changed contact for org: %d", org.OrgID())
	}
	defer rows.Close()
	if err != sql.ErrNoRows {
		rows.Next()
		var newest time.Time
		err = rows.Scan(&newest)
		if err != nil {
			return 0, errors.Wrapf(err, "error scanning most recent contact modified_on for org: %d", org.OrgID())
		}

		// if it was more recent than 10 seconds ago, sleep until it has been 10 seconds
		if newest.Add(time.Second * 10).After(start) {
			sleep := newest.Add(time.Second * 10).Sub(start)
			logrus.WithField("sleep", sleep).Info("sleeping before evaluating dynamic group")
			time.Sleep(sleep)
		}
	}

	// get current set of ids in our group
	ids, err := ContactIDsForGroupIDs(ctx, db, []GroupID{groupID})
	if err != nil {
		return 0, errors.Wrapf(err, "unable to look up contact ids for group: %d", groupID)
	}
	present := make(map[ContactID]bool, len(ids))
	for _, i := range ids {
		present[i] = true
	}

	// calculate new set of ids
	new, err := ContactIDsForQuery(ctx, es, org, query)
	if err != nil {
		return 0, errors.Wrapf(err, "error performing query: %s for group: %d", query, groupID)
	}

	// find which need to be added or removed
	adds := make([]interface{}, 0, 100)
	for _, id := range new {
		if !present[id] {
			adds = append(adds, &GroupAdd{
				GroupID:   groupID,
				ContactID: id,
			})
		}
		delete(present, id)
	}

	// ids that remain need to be removed
	removes := make([]interface{}, 0, 100)
	for id := range present {
		removes = append(removes, &GroupRemove{
			GroupID:   groupID,
			ContactID: id,
		})
	}

	// first remove those needing removal
	err = BatchedBulkSQL(ctx, "remove dynamic group contacts", db, removeContactsFromGroupsSQL, removes, 500)
	if err != nil {
		return 0, errors.Wrapf(err, "error removing contacts from dynamic group: %d", groupID)
	}

	// then add those needing adding
	err = BatchedBulkSQL(ctx, "add dynamic group contacts", db, addContactsToGroupsSQL, adds, 500)
	if err != nil {
		return 0, errors.Wrapf(err, "error adding contacts to dynamic group: %d", groupID)
	}

	// mark our group as no longer evaluating
	err = UpdateGroupStatus(ctx, db, groupID, GroupStatusReady)
	if err != nil {
		return 0, errors.Wrapf(err, "error marking dynamic group as ready")
	}

	return len(new), nil
}
