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

	globals, err := loadGlobals(ctx, tx, 1)
	assert.NoError(t, err)

	assert.Equal(t, 2, len(globals))
	assert.Equal(t, "access_token", globals[0].Key())
	assert.Equal(t, "Access Token", globals[0].Name())
	assert.Equal(t, "A213CD78", globals[0].Value())
	assert.Equal(t, "org_name", globals[1].Key())
	assert.Equal(t, "Org Name", globals[1].Name())
	assert.Equal(t, "Nyaruka", globals[1].Value())
}
