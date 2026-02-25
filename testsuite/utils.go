package testsuite

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/gomodule/redigo/redis"
	"github.com/nyaruka/gocommon/aws/dynamo"
	"github.com/nyaruka/gocommon/aws/dynamo/dyntest"
	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/core/tasks/ctasks"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/testsuite/testdb"
	"github.com/nyaruka/mailroom/utils/queues"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	"github.com/stretchr/testify/require"
)

func QueueBatchTask(t *testing.T, rt *runtime.Runtime, org *testdb.Org, task tasks.Task) {
	ctx := context.Background()

	err := tasks.Queue(ctx, rt, rt.Queues.Batch, org.ID, task, false)
	require.NoError(t, err)
}

func QueueContactTask(t *testing.T, rt *runtime.Runtime, org *testdb.Org, contact *testdb.Contact, ctask ctasks.Task) {
	ctx := context.Background()

	err := tasks.QueueContact(ctx, rt, org.ID, contact.ID, ctask)
	require.NoError(t, err)
}

func CurrentTasks(t *testing.T, rt *runtime.Runtime, qname string) map[models.OrgID][]*queues.Task {
	vc := rt.VK.Get()
	defer vc.Close()

	queued, err := redis.Ints(vc.Do("ZRANGE", fmt.Sprintf("{tasks:%s}:queued", qname), 0, -1))
	require.NoError(t, err)

	tasks := make(map[models.OrgID][]*queues.Task)
	for _, orgID := range queued {
		tasks1, err := redis.Strings(vc.Do("LRANGE", fmt.Sprintf("{tasks:%s}:o:%d/1", qname, orgID), 0, -1))
		require.NoError(t, err)

		tasks0, err := redis.Strings(vc.Do("LRANGE", fmt.Sprintf("{tasks:%s}:o:%d/0", qname, orgID), 0, -1))
		require.NoError(t, err)

		orgTasks := make([]*queues.Task, len(tasks1)+len(tasks0))

		for i, rawTask := range slices.Concat(tasks1, tasks0) {
			parts := bytes.SplitN([]byte(rawTask), []byte("|"), 2) // split into id and task json

			task := &queues.Task{}
			jsonx.MustUnmarshal(parts[1], task)
			orgTasks[i] = task
		}

		tasks[models.OrgID(orgID)] = orgTasks
	}

	return tasks
}

type TaskInfo struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

func GetQueuedTasks(t *testing.T, rt *runtime.Runtime) map[string][]TaskInfo {
	t.Helper()

	actual := make(map[string][]TaskInfo)

	for _, qname := range []string{"realtime", "batch", "throttled"} {
		for orgID, oTasks := range CurrentTasks(t, rt, qname) {
			key := fmt.Sprintf("%s/%d", qname, orgID)
			actual[key] = make([]TaskInfo, len(oTasks))
			for i, task := range oTasks {
				actual[key][i] = TaskInfo{Type: task.Type, Payload: task.Task}
			}
		}
	}

	return actual
}

func GetQueuedTaskTypes(t *testing.T, rt *runtime.Runtime) map[string][]string {
	t.Helper()

	actual := make(map[string][]string)

	for key, tasks := range GetQueuedTasks(t, rt) {
		types := make([]string, len(tasks))
		for i, task := range tasks {
			types[i] = task.Type
		}
		actual[key] = types
	}

	return actual
}

// FlushTasks processes any queued tasks
func FlushTasks(t *testing.T, rt *runtime.Runtime, qnames ...string) map[string]int {
	return drainTasks(t, rt, true, qnames...)
}

// ClearTasks removes any queued tasks without processing them
func ClearTasks(t *testing.T, rt *runtime.Runtime, qnames ...string) map[string]int {
	return drainTasks(t, rt, false, qnames...)
}

func drainTasks(t *testing.T, rt *runtime.Runtime, perform bool, qnames ...string) map[string]int {
	vc := rt.VK.Get()
	defer vc.Close()

	var task *queues.Task
	var err error
	counts := make(map[string]int)

	var qs []queues.Fair
	for _, q := range []queues.Fair{rt.Queues.Realtime, rt.Queues.Batch, rt.Queues.Throttled} {
		if len(qnames) == 0 || slices.Contains(qnames, fmt.Sprint(q)) {
			qs = append(qs, q)
		}
	}

	for {
		// look for a task in the queues
		var q queues.Fair
		for _, q = range qs {
			task, err = q.Pop(t.Context(), vc)
			require.NoError(t, err)

			if task != nil {
				break
			}
		}

		if task == nil { // all done
			break
		}

		counts[task.Type]++

		if perform {
			err = tasks.Perform(t.Context(), rt, task)
			require.NoError(t, err, "unexpected error performing task %s", task.Type)
		}

		err = q.Done(t.Context(), vc, task.OwnerID)
		require.NoError(t, err, "unexpected error marking task %s as done", task.Type)
	}
	return counts
}

// IndexedMessage represents an indexed OpenSearch message for test assertions, including metadata
// fields (_id and _routing) that aren't part of the document body.
type IndexedMessage struct {
	ID          string `json:"_id"`
	Routing     string `json:"_routing"`
	ContactUUID string `json:"contact_uuid"`
	Text        string `json:"text"`
}

func GetIndexedMessages(t *testing.T, rt *runtime.Runtime, clear bool) []IndexedMessage {
	t.Helper()

	rt.OS.Writer.Flush()

	client := rt.OS.Client
	pattern := rt.Config.OSMessagesIndex + "-*"

	// refresh the indexes to make documents searchable
	refreshResp, err := client.Indices.Refresh(t.Context(), &opensearchapi.IndicesRefreshReq{Indices: []string{pattern}})
	if err != nil || refreshResp.Inspect().Response.IsError() {
		return nil // no matching indexes yet
	}

	// search all documents, sorted by _id for deterministic ordering
	resp, err := client.Search(t.Context(), &opensearchapi.SearchReq{
		Indices: []string{pattern},
		Body:    strings.NewReader(`{"query": {"match_all": {}}, "sort": ["_id"]}`),
	})
	require.NoError(t, err)

	msgs := make([]IndexedMessage, len(resp.Hits.Hits))
	for i, hit := range resp.Hits.Hits {
		err := json.Unmarshal(hit.Source, &msgs[i])
		require.NoError(t, err)
		msgs[i].ID = hit.ID
		msgs[i].Routing = hit.Routing
	}

	if clear {
		// delete all matching monthly indexes
		client.Indices.Delete(t.Context(), opensearchapi.IndicesDeleteReq{Indices: []string{pattern}})
	}

	return msgs
}

func GetHistoryItems(t *testing.T, rt *runtime.Runtime, clear bool, after time.Time) []*dynamo.Item {
	t.Helper()

	rt.Dynamo.History.Flush()

	allItems := dyntest.ScanAll(t, rt.Dynamo.History.Client(), "TestHistory")

	if after.IsZero() {
		if clear {
			dyntest.Truncate(t, rt.Dynamo.History.Client(), "TestHistory")
		}
		return allItems
	}

	// filter items by UUID7 time boundary
	afterMs := after.UnixMilli()
	items := make([]*dynamo.Item, 0, len(allItems))

	for _, item := range allItems {
		if skUUID7TimeMs(item.SK) >= afterMs {
			items = append(items, item)
		}
	}

	if clear && len(items) > 0 {
		client := rt.Dynamo.History.Client()
		table := rt.Dynamo.History.Table()
		for _, item := range items {
			_, err := client.DeleteItem(t.Context(), &dynamodb.DeleteItemInput{
				TableName: aws.String(table),
				Key: map[string]dbtypes.AttributeValue{
					"PK": &dbtypes.AttributeValueMemberS{Value: item.PK},
					"SK": &dbtypes.AttributeValueMemberS{Value: item.SK},
				},
			})
			require.NoError(t, err)
		}
	}

	return items
}

// skUUID7TimeMs extracts the millisecond timestamp from a sort key like "evt#<uuid7>" or "evt#<uuid7>#<tag>"
func skUUID7TimeMs(sk string) int64 {
	if len(sk) < 40 { // "evt#" (4) + UUID (36)
		return 0
	}
	// UUID7 first 48 bits = 12 hex chars at positions 0-7 and 9-12 of the UUID
	hex := sk[4:12] + sk[13:17]
	ms, err := strconv.ParseInt(hex, 16, 64)
	if err != nil {
		return 0
	}
	return ms
}

func GetHistoryEventTypes(t *testing.T, rt *runtime.Runtime, clear bool, after time.Time) map[flows.ContactUUID][]string {
	items := GetHistoryItems(t, rt, clear, after)

	evtTypes := make(map[flows.ContactUUID][]string, len(items))

	for _, item := range items {
		data, err := item.GetData()
		require.NoError(t, err)

		evtType, ok := data["type"]
		if ok {
			contactUUID := flows.ContactUUID(item.PK)[4:] // trim off con#
			evtTypes[contactUUID] = append(evtTypes[contactUUID], evtType.(string))
		}
	}

	return evtTypes
}
