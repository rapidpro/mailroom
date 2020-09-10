package dbutil_test

import (
	"testing"

	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/utils/dbutil"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
)

func TestBulkSQL(t *testing.T) {
	db := testsuite.DB()

	type contact struct {
		ID   int    `db:"id"`
		Name string `db:"name"`
	}

	_, _, err := dbutil.BulkSQL(db, `UPDATE contact_contact SET name = :name WHERE id = :id`, []interface{}{contact{ID: 1, Name: "Bob"}})
	assert.EqualError(t, err, "error extracting VALUES from sql: UPDATE contact_contact SET name = ? WHERE id = ?")

	sql := `INSERT INTO contacts_contact (id, name) VALUES(:id, :name)`

	// try with zero structs
	query, args, err := dbutil.BulkSQL(db, sql, []interface{}{})
	assert.EqualError(t, err, "can't generate bulk sql with zero structs")

	// try with one struct
	query, args, err = dbutil.BulkSQL(db, sql, []interface{}{contact{ID: 1, Name: "Bob"}})
	assert.NoError(t, err)
	assert.Equal(t, `INSERT INTO contacts_contact (id, name) VALUES($1, $2)`, query)
	assert.Equal(t, []interface{}{1, "Bob"}, args)

	// try with multiple...
	query, args, err = dbutil.BulkSQL(db, sql, []interface{}{contact{ID: 1, Name: "Bob"}, contact{ID: 2, Name: "Cathy"}, contact{ID: 3, Name: "George"}})
	assert.NoError(t, err)
	assert.Equal(t, `INSERT INTO contacts_contact (id, name) VALUES($1, $2),($3, $4),($5, $6)`, query)
	assert.Equal(t, []interface{}{1, "Bob", 2, "Cathy", 3, "George"}, args)
}

func TestBulkQuery(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()
	defer testsuite.Reset()

	db.MustExec(`CREATE TABLE foo (id serial NOT NULL PRIMARY KEY, name TEXT, age INT)`)

	type foo struct {
		ID   int    `db:"id"`
		Name string `db:"name"`
		Age  int    `db:"age"`
	}

	sql := `INSERT INTO foo (name, age) VALUES(:name, :age) RETURNING id`

	// noop with zero structs
	err := dbutil.BulkQuery(ctx, db, sql, nil)
	assert.NoError(t, err)

	// returned ids are scanned into structs
	foo1 := &foo{Name: "Bob", Age: 64}
	foo2 := &foo{Name: "Jon", Age: 34}
	err = dbutil.BulkQuery(ctx, db, sql, []interface{}{foo1, foo2})
	assert.NoError(t, err)
	assert.Equal(t, 1, foo1.ID)
	assert.Equal(t, 2, foo2.ID)
	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM foo WHERE name = 'Bob' AND age = 64`, nil, 1)
	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM foo WHERE name = 'Jon' AND age = 34`, nil, 1)

	// returning ids is optional
	foo3 := &foo{Name: "Jim", Age: 54}
	err = dbutil.BulkQuery(ctx, db, `INSERT INTO foo (name, age) VALUES(:name, :age)`, []interface{}{foo3})
	assert.NoError(t, err)
	assert.Equal(t, 0, foo3.ID)
	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM foo WHERE name = 'Jim' AND age = 54`, nil, 1)
}
