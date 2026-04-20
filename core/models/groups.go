package models

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/nyaruka/gocommon/dbutil"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/pkg/errors"
)

// GroupID is our type for group ids
type GroupID int

// GroupStatus is the current status of the passed in group
type GroupStatus string

const (
	GroupStatusInitializing = GroupStatus("I")
	GroupStatusEvaluating   = GroupStatus("V")
	GroupStatusReady        = GroupStatus("R")
)

// GroupType is the the type of a group
type GroupType string

const (
	GroupTypeManual = GroupType("M")
	GroupTypeSmart  = GroupType("Q")
)

// Group is our mailroom type for contact groups
type Group struct {
	ID_     GroupID          `json:"id"`
	UUID_   assets.GroupUUID `json:"uuid"`
	Name_   string           `json:"name"`
	Query_  string           `json:"query"`
	Status_ GroupStatus      `json:"status"`
	Type_   GroupType        `json:"group_type"`
}

// ID returns the ID for this group
func (g *Group) ID() GroupID { return g.ID_ }

// UUID returns the uuid for this group
func (g *Group) UUID() assets.GroupUUID { return g.UUID_ }

// Name returns the name for this group
func (g *Group) Name() string { return g.Name_ }

// Query returns the query string (if any) for this group
func (g *Group) Query() string { return g.Query_ }

// Status returns the status of this group
func (g *Group) Status() GroupStatus { return g.Status_ }

// Type returns the type of this group
func (g *Group) Type() GroupType { return g.Type_ }

// loads the groups for the passed in org
func loadGroups(ctx context.Context, db *sql.DB, orgID OrgID) ([]assets.Group, error) {
	rows, err := db.QueryContext(ctx, sqlSelectGroupsByOrg, orgID)
	if err != nil {
		return nil, errors.Wrapf(err, "error querying groups for org: %d", orgID)
	}

	return ScanJSONRows(rows, func() assets.Group { return &Group{} })
}

const sqlSelectGroupsByOrg = `
SELECT ROW_TO_JSON(r) FROM (
      SELECT id, uuid, name, query, status, group_type
        FROM contacts_contactgroup 
       WHERE org_id = $1 AND is_active = TRUE
    ORDER BY name ASC
) r;`

// RemoveContactsFromGroups fires a bulk SQL query to remove all the contacts in the passed in groups
func RemoveContactsFromGroups(ctx context.Context, tx DBorTx, removals []*GroupRemove) error {
	return BulkQuery(ctx, "removing contacts from groups", tx, removeContactsFromGroupsSQL, removals)
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
func AddContactsToGroups(ctx context.Context, tx DBorTx, adds []*GroupAdd) error {
	return BulkQuery(ctx, "adding contacts to groups", tx, sqlAddContactsToGroups, adds)
}

// GroupAdd is our struct to track a final group additions
type GroupAdd struct {
	ContactID ContactID `db:"contact_id"`
	GroupID   GroupID   `db:"group_id"`
}

const sqlAddContactsToGroups = `
INSERT INTO contacts_contactgroup_contacts(contact_id, contactgroup_id)
                                    VALUES(:contact_id, :group_id)
ON CONFLICT DO NOTHING`

// ContactIDsForGroupIDs returns the unique contacts that are in the passed in groups
func ContactIDsForGroupIDs(ctx context.Context, tx DBorTx, groupIDs []GroupID) ([]ContactID, error) {
	// now add all the ids for our groups
	rows, err := tx.QueryContext(ctx, `SELECT DISTINCT(contact_id) FROM contacts_contactgroup_contacts WHERE contactgroup_id = ANY($1)`, pq.Array(groupIDs))
	if err != nil {
		return nil, errors.Wrapf(err, "error selecting contacts for groups")
	}

	contactIDs := make([]ContactID, 0, 10)

	contactIDs, err = dbutil.ScanAllSlice(rows, contactIDs)
	if err != nil {
		return nil, errors.Wrap(err, "error scanning contact ids")
	}
	return contactIDs, nil
}

const updateGroupStatusSQL = `UPDATE contacts_contactgroup SET status = $2 WHERE id = $1`

// UpdateGroupStatus updates the group status for the passed in group
func UpdateGroupStatus(ctx context.Context, db DBorTx, groupID GroupID, status GroupStatus) error {
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
