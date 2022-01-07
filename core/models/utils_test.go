package models_test

import (
	"testing"

	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"

	"github.com/stretchr/testify/assert"
)

func TestBulkQueryBatches(t *testing.T) {
	ctx, _, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetAll)

	db.MustExec(`CREATE TABLE foo (id serial NOT NULL PRIMARY KEY, name TEXT, age INT)`)

	type foo struct {
		ID   int    `db:"id"`
		Name string `db:"name"`
		Age  int    `db:"age"`
	}

	sql := `INSERT INTO foo (name, age) VALUES(:name, :age) RETURNING id`

	// noop with zero structs
	err := models.BulkQueryBatches(ctx, "foo inserts", db, sql, 10, nil)
	assert.NoError(t, err)

	// test when structs fit into one batch
	foo1 := &foo{Name: "A", Age: 30}
	foo2 := &foo{Name: "B", Age: 31}
	err = models.BulkQueryBatches(ctx, "foo inserts", db, sql, 2, []interface{}{foo1, foo2})
	assert.NoError(t, err)
	assert.Equal(t, 1, foo1.ID)
	assert.Equal(t, 2, foo2.ID)
	assertdb.Query(t, db, `SELECT count(*) FROM foo WHERE name = 'A' AND age = 30`).Returns(1)
	assertdb.Query(t, db, `SELECT count(*) FROM foo WHERE name = 'B' AND age = 31`).Returns(1)

	// test when multiple batches are required
	foo3 := &foo{Name: "C", Age: 32}
	foo4 := &foo{Name: "D", Age: 33}
	foo5 := &foo{Name: "E", Age: 34}
	foo6 := &foo{Name: "F", Age: 35}
	foo7 := &foo{Name: "G", Age: 36}
	err = models.BulkQueryBatches(ctx, "foo inserts", db, sql, 2, []interface{}{foo3, foo4, foo5, foo6, foo7})
	assert.NoError(t, err)
	assert.Equal(t, 3, foo3.ID)
	assert.Equal(t, 4, foo4.ID)
	assert.Equal(t, 5, foo5.ID)
	assert.Equal(t, 6, foo6.ID)
	assert.Equal(t, 7, foo7.ID)
	assertdb.Query(t, db, `SELECT count(*) FROM foo WHERE name = 'C' AND age = 32`).Returns(1)
	assertdb.Query(t, db, `SELECT count(*) FROM foo WHERE name = 'D' AND age = 33`).Returns(1)
	assertdb.Query(t, db, `SELECT count(*) FROM foo WHERE name = 'E' AND age = 34`).Returns(1)
	assertdb.Query(t, db, `SELECT count(*) FROM foo WHERE name = 'F' AND age = 35`).Returns(1)
	assertdb.Query(t, db, `SELECT count(*) FROM foo WHERE name = 'G' AND age = 36`).Returns(1)
	assertdb.Query(t, db, `SELECT count(*) FROM foo `).Returns(7)
}
