package testdata

import (
	"testing"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/null"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

// InsertContactGroup inserts a contact group
func InsertContactGroup(t *testing.T, db *sqlx.DB, orgID models.OrgID, name, query string) models.GroupID {
	var id models.GroupID
	err := db.Get(&id,
		`INSERT INTO contacts_contactgroup(uuid, org_id, group_type, name, query, status, is_active, created_by_id, created_on, modified_by_id, modified_on) 
		 VALUES($1, $2, 'U', $3, $4, 'R', TRUE, 1, NOW(), 1, NOW()) RETURNING id`, uuids.New(), models.Org1, name, null.String(query),
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
