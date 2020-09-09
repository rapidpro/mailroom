package dbutil_test

import (
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/utils/dbutil"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadJSONRow(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()

	type group struct {
		UUID string `json:"uuid"`
		Name string `json:"name"`
	}

	queryRows := func(sql string, args ...interface{}) *sqlx.Rows {
		rows, err := db.QueryxContext(ctx, sql, args...)
		require.NoError(t, err)
		require.True(t, rows.Next())
		return rows
	}

	// if query returns valid JSON which can be unmarshaled into our struct, all good
	rows := queryRows(`SELECT ROW_TO_JSON(r) FROM (SELECT g.uuid as uuid, g.name AS name FROM contacts_contactgroup g WHERE id = $1) r`, models.TestersGroupID)

	g := &group{}
	err := dbutil.ReadJSONRow(rows, g)
	assert.NoError(t, err)
	assert.Equal(t, "5e9d8fab-5e7e-4f51-b533-261af5dea70d", g.UUID)
	assert.Equal(t, "Testers", g.Name)

	// error if row value is not JSON
	rows = queryRows(`SELECT id FROM contacts_contactgroup g WHERE id = $1`, models.TestersGroupID)
	err = dbutil.ReadJSONRow(rows, g)
	assert.EqualError(t, err, "error unmarshalling row JSON: json: cannot unmarshal number into Go value of type dbutil_test.group")

	// error if rows aren't ready to be scanned - e.g. next hasn't been called
	rows, err = db.QueryxContext(ctx, `SELECT ROW_TO_JSON(r) FROM (SELECT g.uuid as uuid, g.name AS name FROM contacts_contactgroup g WHERE id = $1) r`, models.TestersGroupID)
	require.NoError(t, err)
	err = dbutil.ReadJSONRow(rows, g)
	assert.EqualError(t, err, "error scanning row JSON: sql: Scan called without calling Next")
}
