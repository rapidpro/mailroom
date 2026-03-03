package models

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/nyaruka/gocommon/dbutil"
	"github.com/nyaruka/goflow/assets"
)

// GroupID is our type for group ids
type GroupID int

// GroupStatus is the current status of the passed in group
type GroupStatus string

const (
	GroupStatusInitializing = GroupStatus("I")
	GroupStatusEvaluating   = GroupStatus("V")
	GroupStatusReady        = GroupStatus("R")
	GroupStatusInvalid      = GroupStatus("X")
)

// GroupType is the the type of a group
type GroupType string

const (
	GroupTypeDBActive   = GroupType("A")
	GroupTypeDBBlocked  = GroupType("B")
	GroupTypeDBStopped  = GroupType("S")
	GroupTypeDBArchived = GroupType("V")
	GroupTypeManual     = GroupType("M")
	GroupTypeSmart      = GroupType("Q")
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

// Visible returns whether this group is visible to the engine (status groups are not)
func (g *Group) Visible() bool { return g.Type_ == GroupTypeManual || g.Type_ == GroupTypeSmart }

// loads the groups for the passed in org
func loadGroups(ctx context.Context, db *sql.DB, orgID OrgID) ([]assets.Group, error) {
	rows, err := db.QueryContext(ctx, sqlSelectGroupsByOrg, orgID)
	if err != nil {
		return nil, fmt.Errorf("error querying groups for org: %d: %w", orgID, err)
	}

	return ScanJSONRows(rows, func() assets.Group { return &Group{} })
}

const sqlSelectGroupsByOrg = `
SELECT ROW_TO_JSON(r) FROM (
      SELECT id, uuid, name, query, status, group_type
        FROM contacts_contactgroup 
       WHERE org_id = $1 AND is_active = TRUE AND status != 'X'
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
DELETE FROM contacts_contactgroup_contacts
WHERE id IN (
	SELECT c.id 
	FROM contacts_contactgroup_contacts c, (VALUES(:contact_id::int, :group_id::int)) AS g(contact_id, group_id)
	WHERE c.contact_id = g.contact_id AND c.contactgroup_id = g.group_id
);`

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

// GetGroupContactCount returns the total number of contacts that are in given group
func GetGroupContactCount(ctx context.Context, db *sql.DB, groupID GroupID) (int, error) {
	var count int
	err := db.QueryRowContext(ctx, `SELECT SUM(count) FROM contacts_contactgroupcount WHERE group_id = $1 GROUP BY group_id`, groupID).Scan(&count)
	if err != nil && err != sql.ErrNoRows {
		return 0, fmt.Errorf("error getting group contact count: %w", err)
	}
	return count, nil
}

// GetGroupContactIDs returns the ids of the contacts that are in given group
func GetGroupContactIDs(ctx context.Context, tx DBorTx, groupID GroupID) ([]ContactID, error) {
	rows, err := tx.QueryContext(ctx, `SELECT contact_id FROM contacts_contactgroup_contacts WHERE contactgroup_id = $1`, groupID)
	if err != nil {
		return nil, fmt.Errorf("error selecting contact ids for group: %w", err)
	}

	contactIDs := make([]ContactID, 0, 10)

	contactIDs, err = dbutil.ScanAllSlice(rows, contactIDs)
	if err != nil {
		return nil, fmt.Errorf("error scanning contact ids: %w", err)
	}
	return contactIDs, nil
}

const updateGroupStatusSQL = `UPDATE contacts_contactgroup SET status = $2 WHERE id = $1`

// UpdateGroupStatus updates the group status for the passed in group
func UpdateGroupStatus(ctx context.Context, db DBorTx, groupID GroupID, status GroupStatus) error {
	_, err := db.ExecContext(ctx, updateGroupStatusSQL, groupID, status)
	if err != nil {
		return fmt.Errorf("error updating group status for group: %d: %w", groupID, err)
	}
	return nil
}

