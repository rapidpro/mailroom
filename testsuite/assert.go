package testsuite

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/gomodule/redigo/redis"
	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/goflow/test"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/queue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// AssertCourierQueues asserts the sizes of message batches in the named courier queues
func AssertCourierQueues(t *testing.T, expected map[string][]int, errMsg ...any) {
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
func AssertContactTasks(t *testing.T, orgID models.OrgID, contactID models.ContactID, expected []string, msgAndArgs ...any) {
	rc := getRC()
	defer rc.Close()

	tasks, err := redis.Strings(rc.Do("LRANGE", fmt.Sprintf("c:%d:%d", orgID, contactID), 0, -1))
	require.NoError(t, err)

	expectedJSON := jsonx.MustMarshal(expected)
	actualJSON := jsonx.MustMarshal(tasks)

	test.AssertEqualJSON(t, expectedJSON, actualJSON, "")
}

// AssertBatchTasks asserts that the given org has the given batch tasks queued for them
func AssertBatchTasks(t *testing.T, orgID models.OrgID, expected map[string]int, msgAndArgs ...any) {
	rc := getRC()
	defer rc.Close()

	tasks, err := redis.Strings(rc.Do("ZRANGE", fmt.Sprintf("batch:%d", orgID), 0, -1))
	require.NoError(t, err)

	actual := make(map[string]int, 5)
	for _, taskJSON := range tasks {
		task := &queue.Task{}
		jsonx.MustUnmarshal(json.RawMessage(taskJSON), task)

		actual[task.Type] += 1
	}

	assert.Equal(t, expected, actual, msgAndArgs...)
}
