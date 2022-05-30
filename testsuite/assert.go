package testsuite

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/goflow/test"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// AssertCourierQueues asserts the sizes of message batches in the named courier queues
func AssertCourierQueues(t *testing.T, expected map[string][]int, errMsg ...interface{}) {
	rc := getRC()
	defer rc.Close()

	queueKeys, err := redis.Strings(rc.Do("KEYS", "msgs:????????-*"))
	require.NoError(t, err)

	actual := make(map[string][]int, len(queueKeys))
	for _, queueKey := range queueKeys {
		size, err := redis.Int64(rc.Do("ZCARD", queueKey))
		require.NoError(t, err)
		actual[queueKey] = make([]int, size)

		if size > 0 {
			results, err := redis.Values(rc.Do("ZRANGE", queueKey, 0, -1, "WITHSCORES"))
			require.NoError(t, err)
			require.Equal(t, int(size*2), len(results)) // result is (item, score, item, score, ...)

			// unmarshal each item in the queue as a batch of messages
			for i := 0; i < int(size); i++ {
				batchJSON := results[i*2].([]byte)
				var batch []map[string]interface{}
				err = json.Unmarshal(batchJSON, &batch)
				require.NoError(t, err)

				actual[queueKey][i] = len(batch)
			}
		}
	}

	assert.Equal(t, expected, actual, errMsg...)
}

// AssertContactTasks asserts that the given contact has the given tasks queued for them
func AssertContactTasks(t *testing.T, orgID models.OrgID, contactID models.ContactID, expected []string, msgAndArgs ...interface{}) {
	rc := getRC()
	defer rc.Close()

	tasks, err := redis.Strings(rc.Do("LRANGE", fmt.Sprintf("c:%d:%d", orgID, contactID), 0, -1))
	require.NoError(t, err)

	expectedJSON := jsonx.MustMarshal(expected)
	actualJSON := jsonx.MustMarshal(tasks)

	test.AssertEqualJSON(t, expectedJSON, actualJSON, "")
}

// AssertQuery creates a new query on which one can assert things
func AssertQuery(t *testing.T, db *sqlx.DB, sql string, args ...interface{}) *Query {
	return &Query{t, db, sql, args}
}

type Query struct {
	t    *testing.T
	db   *sqlx.DB
	sql  string
	args []interface{}
}

func (q *Query) Returns(expected interface{}, msgAndArgs ...interface{}) {
	q.t.Helper()

	// get a variable of same type to hold actual result
	actual := expected

	err := q.db.Get(&actual, q.sql, q.args...)
	assert.NoError(q.t, err, msgAndArgs...)

	// not sure why but if you pass an int you get back an int64..
	switch expected.(type) {
	case int:
		actual = int(actual.(int64))
	}

	assert.Equal(q.t, expected, actual, msgAndArgs...)
}

func (q *Query) Columns(expected map[string]interface{}, msgAndArgs ...interface{}) {
	q.t.Helper()

	actual := make(map[string]interface{}, len(expected))

	err := q.db.QueryRowx(q.sql, q.args...).MapScan(actual)
	assert.NoError(q.t, err, msgAndArgs...)
	assert.Equal(q.t, expected, actual, msgAndArgs...)
}
