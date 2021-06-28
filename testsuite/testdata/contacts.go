package testdata

import (
	"testing"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/null"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

// InsertContact inserts a contact
func InsertContact(t *testing.T, db *sqlx.DB, orgID models.OrgID, uuid flows.ContactUUID, name string, language envs.Language) models.ContactID {
	var id models.ContactID
	err := db.Get(&id,
		`INSERT INTO contacts_contact (org_id, is_active, status, uuid, name, language, created_on, modified_on, created_by_id, modified_by_id) 
		VALUES($1, TRUE, 'A', $2, $3, $4, NOW(), NOW(), 1, 1) RETURNING id`, orgID, uuid, name, language,
	)
	require.NoError(t, err)
	return id
}

// InsertContactGroup inserts a contact group
func InsertContactGroup(t *testing.T, db *sqlx.DB, orgID models.OrgID, uuid assets.GroupUUID, name, query string) models.GroupID {
	var id models.GroupID
	err := db.Get(&id,
		`INSERT INTO contacts_contactgroup(uuid, org_id, group_type, name, query, status, is_active, created_by_id, created_on, modified_by_id, modified_on) 
		 VALUES($1, $2, 'U', $3, $4, 'R', TRUE, 1, NOW(), 1, NOW()) RETURNING id`, uuid, models.Org1, name, null.String(query),
	)
	require.NoError(t, err)
	return id
}

// InsertContactURN inserts a contact URN
func InsertContactURN(t *testing.T, db *sqlx.DB, orgID models.OrgID, contactID models.ContactID, urn urns.URN, priority int) models.URNID {
	scheme, path, _, _ := urn.ToParts()

	var id models.URNID
	err := db.Get(&id,
		`INSERT INTO contacts_contacturn(org_id, contact_id, scheme, path, identity, priority) 
		 VALUES($1, $2, $3, $4, $5, $6) RETURNING id`, orgID, contactID, scheme, path, urn.Identity(), priority,
	)
	require.NoError(t, err)
	return id
}

// DeleteContactsAndURNs deletes all contacts and URNs
func DeleteContactsAndURNs(t *testing.T, db *sqlx.DB) {
	db.MustExec(`DELETE FROM contacts_contacturn`)
	db.MustExec(`DELETE FROM contacts_contactgroup_contacts`)
	db.MustExec(`DELETE FROM contacts_contact`)

	// reset id sequences back to a known number
	db.MustExec(`ALTER SEQUENCE contacts_contact_id_seq RESTART WITH 10000`)
	db.MustExec(`ALTER SEQUENCE contacts_contacturn_id_seq RESTART WITH 10000`)
}
