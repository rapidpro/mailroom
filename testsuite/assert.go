package testsuite

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"testing"
	"time"

	valkey "github.com/gomodule/redigo/redis"
	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/goflow/test"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/testsuite/testdb"
	"github.com/nyaruka/mailroom/utils/queues"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// AssertCourierQueues asserts the sizes of message batches in the named courier queues
func AssertCourierQueues(t *testing.T, rt *runtime.Runtime, expected map[string][]int, errMsg ...any) {
	vc := rt.VK.Get()
	defer vc.Close()

	queueKeys, err := valkey.Strings(vc.Do("KEYS", "msgs:????????-*"))
	require.NoError(t, err)

	actual := make(map[string][]int, len(queueKeys))
	for _, queueKey := range queueKeys {
		size, err := valkey.Int64(vc.Do("ZCARD", queueKey))
		require.NoError(t, err)
		actual[queueKey] = make([]int, size)

		if size > 0 {
			results, err := valkey.Values(vc.Do("ZRANGE", queueKey, 0, -1, "WITHSCORES"))
			require.NoError(t, err)
			require.Equal(t, int(size*2), len(results)) // result is (item, score, item, score, ...)

			// unmarshal each item in the queue as a batch of messages
			for i := 0; i < int(size); i++ {
				batchJSON := results[i*2].([]byte)
				var batch []map[string]any
				err = json.Unmarshal(batchJSON, &batch)
				require.NoError(t, err)

				actual[queueKey][i] = len(batch)
			}
		}
	}

	assert.Equal(t, expected, actual, errMsg...)
}

// AssertContactTasks asserts that the given contact has the given tasks queued for them
func AssertContactTasks(t *testing.T, rt *runtime.Runtime, org *testdb.Org, contact *testdb.Contact, expected []string, msgAndArgs ...any) {
	vc := rt.VK.Get()
	defer vc.Close()

	tasks, err := valkey.Strings(vc.Do("LRANGE", fmt.Sprintf("c:%d:%d", org.ID, contact.ID), 0, -1))
	require.NoError(t, err)

	expectedJSON := jsonx.MustMarshal(expected)
	actualJSON := jsonx.MustMarshal(tasks)

	test.AssertEqualJSON(t, expectedJSON, actualJSON, "")
}

// AssertBatchTasks asserts that the given org has the given batch tasks queued for them
func AssertBatchTasks(t *testing.T, rt *runtime.Runtime, orgID models.OrgID, expected map[string]int, msgAndArgs ...any) {
	vc := rt.VK.Get()
	defer vc.Close()

	tasks0, err := valkey.Strings(vc.Do("LRANGE", fmt.Sprintf("{tasks:batch}:o:%d/0", orgID), 0, -1))
	require.NoError(t, err)

	tasks1, err := valkey.Strings(vc.Do("LRANGE", fmt.Sprintf("{tasks:batch}:o:%d/1", orgID), 0, -1))
	require.NoError(t, err)

	actual := make(map[string]int, 5)
	for _, rawTask := range slices.Concat(tasks0, tasks1) {
		parts := bytes.SplitN([]byte(rawTask), []byte("|"), 2) // split into id and task json

		task := &queues.Task{}
		jsonx.MustUnmarshal(parts[1], task)

		actual[task.Type] += 1
	}

	assert.Equal(t, expected, actual, msgAndArgs...)
}

func AssertContactInFlow(t *testing.T, rt *runtime.Runtime, contact *testdb.Contact, flow *testdb.Flow) {
	// check contact has a single waiting session
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM flows_flowsession WHERE contact_uuid = $1 AND status = 'W'`, contact.UUID).Returns(1)

	// check flow of the waiting session and contact is correct
	assertdb.Query(t, rt.DB, `SELECT current_flow_uuid::text FROM flows_flowsession WHERE contact_uuid = $1 AND status = 'W'`, contact.UUID).Returns(string(flow.UUID))
	assertdb.Query(t, rt.DB, `SELECT current_flow_id FROM contacts_contact WHERE id = $1`, contact.ID).Returns(int64(flow.ID))
}

func AssertContactFires(t *testing.T, rt *runtime.Runtime, contactID models.ContactID, expected map[string]time.Time) {
	var fires []*models.ContactFire
	err := rt.DB.Select(&fires, `SELECT * FROM contacts_contactfire WHERE contact_id = $1`, contactID)
	require.NoError(t, err)

	actual := make(map[string]time.Time, len(fires))
	for _, f := range fires {
		key := string(f.Type)
		if f.Scope != "" {
			key += "/" + f.Scope
		}
		if f.SessionUUID != "" {
			key += ":" + string(f.SessionUUID)
		}
		actual[key] = f.FireOn
	}

	assert.Equal(t, expected, actual)
}

func AssertDailyCounts(t *testing.T, rt *runtime.Runtime, org *testdb.Org, expected map[string]int) {
	var counts []models.DailyCount
	err := rt.DB.Select(&counts, `SELECT day, scope, SUM(count) AS count FROM orgs_dailycount WHERE org_id = $1 GROUP BY day, scope`, org.ID)
	require.NoError(t, err)

	actual := make(map[string]int, len(counts))
	for _, count := range counts {
		actual[fmt.Sprintf("%s/%s", count.Day.String(), count.Scope)] = int(count.Count)
	}

	assert.Equal(t, expected, actual)
}
