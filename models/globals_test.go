package models

import (
	"testing"

	"github.com/nyaruka/mailroom/testsuite"
	"github.com/stretchr/testify/assert"
)

func TestGlobals(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()

	tx, err := db.BeginTxx(ctx, nil)
	assert.NoError(t, err)
	defer tx.Rollback()

	tx.MustExec(`INSERT INTO globals_global(is_active, created_on, modified_on, key, name, value, created_by_id, modified_by_id, org_id)
									 VALUES(TRUE, NOW(), NOW(), 'org_name', 'Org Name', 'Acme Ltd', 1, 1, 1);`)
	tx.MustExec(`INSERT INTO globals_global(is_active, created_on, modified_on, key, name, value, created_by_id, modified_by_id, org_id)
							         VALUES(TRUE, NOW(), NOW(), 'access_token', 'Access Token', 'ab2452', 1, 1, 1);`)

	globals, err := loadGlobals(ctx, tx, 1)
	assert.NoError(t, err)

	assert.Equal(t, 2, len(globals))
	assert.Equal(t, "access_token", globals[0].Key())
	assert.Equal(t, "Access Token", globals[0].Name())
	assert.Equal(t, "ab2452", globals[0].Value())
	assert.Equal(t, "org_name", globals[1].Key())
	assert.Equal(t, "Org Name", globals[1].Name())
	assert.Equal(t, "Acme Ltd", globals[1].Value())
}
