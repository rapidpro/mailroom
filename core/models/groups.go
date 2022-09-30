package models

import (
	"context"
	"time"

	"github.com/nyaruka/gocommon/dbutil"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/olivere/elastic/v7"
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

// LoadGroups loads the groups for the passed in org
func LoadGroups(ctx context.Context, db Queryer, orgID OrgID) ([]assets.Group, error) {
	start := time.Now()

	rows, err := db.QueryxContext(ctx, selectGroupsSQL, orgID)
	if err != nil {
		return nil, errors.Wrapf(err, "error querying groups for org: %d", orgID)
	}
	defer rows.Close()

	groups := make([]assets.Group, 0, 10)
	for rows.Next() {
		group := &Group{}
		err = dbutil.ScanJSON(rows, &group.g)
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
	return BulkQuery(ctx, "removing contacts from groups", tx, removeContactsFromGroupsSQL, is)
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
	return BulkQuery(ctx, "adding contacts to groups", tx, addContactsToGroupsSQL, is)
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

// RemoveContactsFromGroupAndCampaigns removes the passed in contacts from the passed in group, taking care of also
// removing them from any associated campaigns
func RemoveContactsFromGroupAndCampaigns(ctx context.Context, db *sqlx.DB, oa *OrgAssets, groupID GroupID, contactIDs []ContactID) error {
	removeBatch := func(batch []ContactID) error {
		tx, err := db.BeginTxx(ctx, nil)

		if err != nil {
			tx.Rollback()
			return errors.Wrapf(err, "error starting transaction")
		}

		removals := make([]*GroupRemove, len(batch))
		for i, cid := range batch {
			removals[i] = &GroupRemove{
				GroupID:   groupID,
				ContactID: cid,
			}
		}
		err = RemoveContactsFromGroups(ctx, tx, removals)
		if err != nil {
			tx.Rollback()
			return errors.Wrapf(err, "error removing contacts from group: %d", groupID)
		}

		// remove from any campaign events
		err = DeleteUnfiredEventsForGroupRemoval(ctx, tx, oa, batch, groupID)
		if err != nil {
			tx.Rollback()
			return errors.Wrapf(err, "error removing contacts from unfired campaign events for group: %d", groupID)
		}

		err = tx.Commit()
		if err != nil {
			return errors.Wrapf(err, "error commiting batch removal of contacts for group: %d", groupID)
		}

		return nil
	}

	// batch up our contacts for removal, 500 at a time
	batch := make([]ContactID, 0, 100)
	for _, id := range contactIDs {
		batch = append(batch, id)

		if len(batch) == 500 {
			err := removeBatch(batch)
			if err != nil {
				return err
			}
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		err := removeBatch(batch)
		if err != nil {
			return err
		}
	}

	return nil
}

// AddContactsToGroupAndCampaigns takes care of adding the passed in contacts to the passed in group, updating any
// associated campaigns as needed
func AddContactsToGroupAndCampaigns(ctx context.Context, db *sqlx.DB, oa *OrgAssets, groupID GroupID, contactIDs []ContactID) error {
	// we need session assets in order to recalculate campaign events
	addBatch := func(batch []ContactID) error {
		tx, err := db.BeginTxx(ctx, nil)

		if err != nil {
			tx.Rollback()
			return errors.Wrapf(err, "error starting transaction")
		}

		adds := make([]*GroupAdd, len(batch))
		for i, cid := range batch {
			adds[i] = &GroupAdd{
				GroupID:   groupID,
				ContactID: cid,
			}
		}
		err = AddContactsToGroups(ctx, tx, adds)
		if err != nil {
			tx.Rollback()
			return errors.Wrapf(err, "error adding contacts to group: %d", groupID)
		}

		// now load our contacts and add update their campaign events
		contacts, err := LoadContacts(ctx, tx, oa, batch)
		if err != nil {
			tx.Rollback()
			return errors.Wrapf(err, "error loading contacts when adding to group: %d", groupID)
		}

		// convert to flow contacts
		fcs := make([]*flows.Contact, len(contacts))
		for i, c := range contacts {
			fcs[i], err = c.FlowContact(oa)
			if err != nil {
				tx.Rollback()
				return errors.Wrapf(err, "error converting contact to flow contact: %s", c.UUID())
			}
		}

		// schedule any upcoming events that were affected by this group
		err = AddCampaignEventsForGroupAddition(ctx, tx, oa, fcs, groupID)
		if err != nil {
			tx.Rollback()
			return errors.Wrapf(err, "error calculating new campaign events during group addition: %d", groupID)
		}

		err = tx.Commit()
		if err != nil {
			return errors.Wrapf(err, "error commiting batch addition of contacts for group: %d", groupID)
		}

		return nil
	}

	// add our contacts in batches of 500
	batch := make([]ContactID, 0, 500)
	for _, id := range contactIDs {
		batch = append(batch, id)

		if len(batch) == 500 {
			err := addBatch(batch)
			if err != nil {
				return err
			}
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		err := addBatch(batch)
		if err != nil {
			return err
		}
	}

	return nil
}

// PopulateDynamicGroup calculates which members should be part of a group and populates the contacts
// for that group by performing the minimum number of inserts / deletes.
func PopulateDynamicGroup(ctx context.Context, db *sqlx.DB, es *elastic.Client, oa *OrgAssets, groupID GroupID, query string) (int, error) {
	err := UpdateGroupStatus(ctx, db, groupID, GroupStatusEvaluating)
	if err != nil {
		return 0, errors.Wrapf(err, "error marking dynamic group as evaluating")
	}

	start := time.Now()

	// we have a bit of a race with the indexer process.. we want to make sure that any contacts that changed
	// before this group was updated but after the last index are included, so if a contact was modified
	// more recently than 10 seconds ago, we wait that long before starting in populating our group
	newest, err := GetNewestContactModifiedOn(ctx, db, oa)
	if err != nil {
		return 0, errors.Wrapf(err, "error getting most recent contact modified_on for org: %d", oa.OrgID())
	}
	if newest != nil {
		n := *newest

		// if it was more recent than 10 seconds ago, sleep until it has been 10 seconds
		if n.Add(time.Second * 10).After(start) {
			sleep := n.Add(time.Second * 10).Sub(start)
			logrus.WithField("sleep", sleep).Info("sleeping before evaluating dynamic group")
			time.Sleep(sleep)
		}
	}

	// get current set of contacts in our group
	ids, err := ContactIDsForGroupIDs(ctx, db, []GroupID{groupID})
	if err != nil {
		return 0, errors.Wrapf(err, "unable to look up contact ids for group: %d", groupID)
	}
	present := make(map[ContactID]bool, len(ids))
	for _, i := range ids {
		present[i] = true
	}

	// calculate new set of ids
	new, err := GetContactIDsForQuery(ctx, es, oa, query, -1)
	if err != nil {
		return 0, errors.Wrapf(err, "error performing query: %s for group: %d", query, groupID)
	}

	// find which contacts need to be added or removed
	adds := make([]ContactID, 0, 100)
	for _, id := range new {
		if !present[id] {
			adds = append(adds, id)
		}
		delete(present, id)
	}

	// build our list of removals
	removals := make([]ContactID, 0, len(present))
	for id := range present {
		removals = append(removals, id)
	}

	// first remove all the contacts
	err = RemoveContactsFromGroupAndCampaigns(ctx, db, oa, groupID, removals)
	if err != nil {
		return 0, errors.Wrapf(err, "error removing contacts from group: %d", groupID)
	}

	// then add them all
	err = AddContactsToGroupAndCampaigns(ctx, db, oa, groupID, adds)
	if err != nil {
		return 0, errors.Wrapf(err, "error adding contacts to group: %d", groupID)
	}

	// mark our group as no longer evaluating
	err = UpdateGroupStatus(ctx, db, groupID, GroupStatusReady)
	if err != nil {
		return 0, errors.Wrapf(err, "error marking dynamic group as ready")
	}

	// finally update modified_on for all affected contacts to ensure these changes are seen by rp-indexer
	changed := make([]ContactID, 0, len(adds))
	changed = append(changed, adds...)
	changed = append(changed, removals...)

	err = UpdateContactModifiedOn(ctx, db, changed)
	if err != nil {
		return 0, errors.Wrapf(err, "error updating contact modified_on after group population")
	}

	return len(new), nil
}
