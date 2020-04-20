package models

import (
	"context"
	"database/sql"

	"github.com/nyaruka/goflow/utils/uuids"
)

const lookupOrgByTokenSQL = `
SELECT 
  o.id AS id, 
  o.uuid as uuid,
  o.name AS name
FROM 
  orgs_org o
JOIN 
  api_apitoken a
ON 
  a.org_id = o.id
JOIN
  auth_group g
ON
  a.role_id = g.id
WHERE
  a.is_active = TRUE AND
  o.is_active = TRUE AND
  o.uuid = $1::uuid AND
  g.name = $2 AND
  a.key = $3;
`

// OrgReference is just a reference for an org, containing the id, uuid and name for the org
type OrgReference struct {
	ID   OrgID      `db:"id"`
	UUID uuids.UUID `db:"uuid"`
	Name string     `db:"name"`
}

// LookupOrgByToken looks up an OrgReference for the given token and permission
func LookupOrgByToken(ctx context.Context, db Queryer, orgUUID uuids.UUID, permission string, token string) (*OrgReference, error) {
	org := &OrgReference{}
	err := db.GetContext(ctx, org, lookupOrgByTokenSQL, orgUUID, permission, token)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return org, nil
}
