package starts

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/config"
	_ "github.com/nyaruka/mailroom/core/handlers"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/queue"
	"github.com/nyaruka/mailroom/core/runner"
	"github.com/nyaruka/mailroom/testsuite"

	"github.com/olivere/elastic/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStarts(t *testing.T) {
	testsuite.Reset()
	ctx := testsuite.CTX()
	rp := testsuite.RP()
	db := testsuite.DB()
	rc := testsuite.RC()
	defer rc.Close()

	mes := testsuite.NewMockElasticServer()
	defer mes.Close()

	es, err := elastic.NewClient(
		elastic.SetURL(mes.URL()),
		elastic.SetHealthcheck(false),
		elastic.SetSniff(false),
	)
	require.NoError(t, err)

	mr := &mailroom.Mailroom{Config: config.Mailroom, DB: db, RP: rp, ElasticClient: es}

	// convert our single message flow to an actual background flow that shouldn't interrupt
	db.MustExec(`UPDATE flows_flow SET flow_type = 'B' WHERE id = $1`, models.SingleMessageFlowID)

	// insert a flow run for one of our contacts
	// TODO: can be replaced with a normal flow start of another flow once we support flows with waits
	db.MustExec(
		`INSERT INTO flows_flowrun(uuid, status, is_active, created_on, modified_on, expires_on, responded, contact_id, flow_id, org_id)
		                    VALUES($1, 'W', TRUE, now(), now(), now(), FALSE, $2, $3, 1);`, uuids.New(), models.GeorgeID, models.FavoritesFlowID)

	tcs := []struct {
		label                string
		flowID               models.FlowID
		groupIDs             []models.GroupID
		contactIDs           []models.ContactID
		createContact        bool
		query                string
		queryResponse        string
		restartParticipants  models.RestartParticipants
		includeActive        models.IncludeActive
		queue                string
		expectedContactCount int
		expectedBatchCount   int
		expectedTotalCount   int
		expectedStatus       models.StartStatus
		expectedActiveRuns   map[models.FlowID]int
	}{
		{
			label:                "Empty flow start",
			flowID:               models.FavoritesFlowID,
			queue:                queue.BatchQueue,
			expectedContactCount: 0,
			expectedBatchCount:   0,
			expectedTotalCount:   0,
			expectedStatus:       models.StartStatusComplete,
			expectedActiveRuns:   map[models.FlowID]int{models.FavoritesFlowID: 1, models.PickNumberFlowID: 0, models.SingleMessageFlowID: 0},
		},
		{
			label:                "Single group",
			flowID:               models.FavoritesFlowID,
			groupIDs:             []models.GroupID{models.DoctorsGroupID},
			queue:                queue.BatchQueue,
			expectedContactCount: 121,
			expectedBatchCount:   2,
			expectedTotalCount:   121,
			expectedStatus:       models.StartStatusComplete,
			expectedActiveRuns:   map[models.FlowID]int{models.FavoritesFlowID: 122, models.PickNumberFlowID: 0, models.SingleMessageFlowID: 0},
		},
		{
			label:                "Group and Contact (but all already active)",
			flowID:               models.FavoritesFlowID,
			groupIDs:             []models.GroupID{models.DoctorsGroupID},
			contactIDs:           []models.ContactID{models.CathyID},
			queue:                queue.BatchQueue,
			expectedContactCount: 121,
			expectedBatchCount:   2,
			expectedTotalCount:   0,
			expectedStatus:       models.StartStatusComplete,
			expectedActiveRuns:   map[models.FlowID]int{models.FavoritesFlowID: 122, models.PickNumberFlowID: 0, models.SingleMessageFlowID: 0},
		},
		{
			label:                "Contact restart",
			flowID:               models.FavoritesFlowID,
			contactIDs:           []models.ContactID{models.CathyID},
			restartParticipants:  true,
			includeActive:        true,
			queue:                queue.HandlerQueue,
			expectedContactCount: 1,
			expectedBatchCount:   1,
			expectedTotalCount:   1,
			expectedStatus:       models.StartStatusComplete,
			expectedActiveRuns:   map[models.FlowID]int{models.FavoritesFlowID: 122, models.PickNumberFlowID: 0, models.SingleMessageFlowID: 0},
		},
		{
			label:                "Previous group and one new contact",
			flowID:               models.FavoritesFlowID,
			groupIDs:             []models.GroupID{models.DoctorsGroupID},
			contactIDs:           []models.ContactID{models.BobID},
			queue:                queue.BatchQueue,
			expectedContactCount: 122,
			expectedBatchCount:   2,
			expectedTotalCount:   1,
			expectedStatus:       models.StartStatusComplete,
			expectedActiveRuns:   map[models.FlowID]int{models.FavoritesFlowID: 123, models.PickNumberFlowID: 0, models.SingleMessageFlowID: 0},
		},
		{
			label:                "Single contact, no restart",
			flowID:               models.FavoritesFlowID,
			contactIDs:           []models.ContactID{models.BobID},
			queue:                queue.HandlerQueue,
			expectedContactCount: 1,
			expectedBatchCount:   1,
			expectedTotalCount:   0,
			expectedStatus:       models.StartStatusComplete,
			expectedActiveRuns:   map[models.FlowID]int{models.FavoritesFlowID: 123, models.PickNumberFlowID: 0, models.SingleMessageFlowID: 0},
		},
		{
			label:                "Single contact, include active, but no restart",
			flowID:               models.FavoritesFlowID,
			contactIDs:           []models.ContactID{models.BobID},
			includeActive:        true,
			queue:                queue.HandlerQueue,
			expectedContactCount: 1,
			expectedBatchCount:   1,
			expectedTotalCount:   0,
			expectedStatus:       models.StartStatusComplete,
			expectedActiveRuns:   map[models.FlowID]int{models.FavoritesFlowID: 123, models.PickNumberFlowID: 0, models.SingleMessageFlowID: 0},
		},
		{
			label:                "Single contact, include active and restart",
			flowID:               models.FavoritesFlowID,
			contactIDs:           []models.ContactID{models.BobID},
			restartParticipants:  true,
			includeActive:        true,
			queue:                queue.HandlerQueue,
			expectedContactCount: 1,
			expectedBatchCount:   1,
			expectedTotalCount:   1,
			expectedStatus:       models.StartStatusComplete,
			expectedActiveRuns:   map[models.FlowID]int{models.FavoritesFlowID: 123, models.PickNumberFlowID: 0, models.SingleMessageFlowID: 0},
		},
		{
			label:  "Query start",
			flowID: models.FavoritesFlowID,
			query:  "bob",
			queryResponse: fmt.Sprintf(`{
			"_scroll_id": "DXF1ZXJ5QW5kRmV0Y2gBAAAAAAAbgc0WS1hqbHlfb01SM2lLTWJRMnVOSVZDdw==",
			"took": 2,
			"timed_out": false,
			"_shards": {
			  "total": 1,
			  "successful": 1,
			  "skipped": 0,
			  "failed": 0
			},
			"hits": {
			  "total": 1,
			  "max_score": null,
			  "hits": [
				{
				  "_index": "contacts",
				  "_type": "_doc",
				  "_id": "%d",
				  "_score": null,
				  "_routing": "1",
				  "sort": [
					15124352
				  ]
				}
			  ]
			}
			}`, models.BobID),
			restartParticipants:  true,
			includeActive:        true,
			queue:                queue.HandlerQueue,
			expectedContactCount: 1,
			expectedBatchCount:   1,
			expectedTotalCount:   1,
			expectedStatus:       models.StartStatusComplete,
			expectedActiveRuns:   map[models.FlowID]int{models.FavoritesFlowID: 123, models.PickNumberFlowID: 0, models.SingleMessageFlowID: 0},
		},
		{
			label:                "Query start with invalid query",
			flowID:               models.FavoritesFlowID,
			query:                "xyz = 45",
			restartParticipants:  true,
			includeActive:        true,
			queue:                queue.HandlerQueue,
			expectedContactCount: 0,
			expectedBatchCount:   0,
			expectedTotalCount:   0,
			expectedStatus:       models.StartStatusFailed,
			expectedActiveRuns:   map[models.FlowID]int{models.FavoritesFlowID: 123, models.PickNumberFlowID: 0, models.SingleMessageFlowID: 0},
		},
		{
			label:                "New Contact",
			flowID:               models.FavoritesFlowID,
			createContact:        true,
			queue:                queue.HandlerQueue,
			expectedContactCount: 1,
			expectedBatchCount:   1,
			expectedTotalCount:   1,
			expectedStatus:       models.StartStatusComplete,
			expectedActiveRuns:   map[models.FlowID]int{models.FavoritesFlowID: 124, models.PickNumberFlowID: 0, models.SingleMessageFlowID: 0},
		},
		{
			label:                "Other messaging flow",
			flowID:               models.PickNumberFlowID,
			contactIDs:           []models.ContactID{models.BobID},
			includeActive:        true,
			queue:                queue.HandlerQueue,
			expectedContactCount: 1,
			expectedBatchCount:   1,
			expectedTotalCount:   1,
			expectedStatus:       models.StartStatusComplete,
			expectedActiveRuns:   map[models.FlowID]int{models.FavoritesFlowID: 123, models.PickNumberFlowID: 1, models.SingleMessageFlowID: 0},
		},
		{
			label:                "Background flow",
			flowID:               models.SingleMessageFlowID,
			contactIDs:           []models.ContactID{models.BobID},
			includeActive:        true,
			queue:                queue.HandlerQueue,
			expectedContactCount: 1,
			expectedBatchCount:   1,
			expectedTotalCount:   1,
			expectedStatus:       models.StartStatusComplete,
			expectedActiveRuns:   map[models.FlowID]int{models.FavoritesFlowID: 123, models.PickNumberFlowID: 1, models.SingleMessageFlowID: 0},
		},
	}

	for _, tc := range tcs {
		mes.NextResponse = tc.queryResponse

		// handle our start task
		start := models.NewFlowStart(models.Org1, models.StartTypeManual, models.FlowTypeMessaging, tc.flowID, tc.restartParticipants, tc.includeActive).
			WithGroupIDs(tc.groupIDs).
			WithContactIDs(tc.contactIDs).
			WithQuery(tc.query).
			WithCreateContact(tc.createContact)

		err := models.InsertFlowStarts(ctx, db, []*models.FlowStart{start})
		assert.NoError(t, err)

		startJSON, err := json.Marshal(start)
		require.NoError(t, err)

		err = handleFlowStart(ctx, mr, &queue.Task{Type: queue.StartFlow, Task: startJSON})
		assert.NoError(t, err)

		// pop all our tasks and execute them
		var task *queue.Task
		count := 0
		for {
			task, err = queue.PopNextTask(rc, tc.queue)
			assert.NoError(t, err)
			if task == nil {
				break
			}

			count++
			assert.Equal(t, queue.StartFlowBatch, task.Type)
			batch := &models.FlowStartBatch{}
			err = json.Unmarshal(task.Task, batch)
			assert.NoError(t, err)

			_, err = runner.StartFlowBatch(ctx, db, rp, batch)
			assert.NoError(t, err)
		}

		// assert our count of batches
		assert.Equal(t, tc.expectedBatchCount, count, "unexpected batch count in '%s'", tc.label)

		// assert our count of total flow runs created
		testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM flows_flowrun WHERE flow_id = $1 AND start_id = $2`,
			[]interface{}{tc.flowID, start.ID()}, tc.expectedTotalCount, "unexpected total run count in '%s'", tc.label)

		// assert final status
		testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM flows_flowstart where status = $2 AND id = $1`,
			[]interface{}{start.ID(), tc.expectedStatus}, 1, "status mismatch in '%s'", tc.label)

		// assert final contact count
		if tc.expectedStatus != models.StartStatusFailed {
			testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM flows_flowstart where contact_count = $2 AND id = $1`,
				[]interface{}{start.ID(), tc.expectedContactCount}, 1, "contact count mismatch in '%s'", tc.label)
		}

		// assert count of active runs by flow
		for flowID, activeRuns := range tc.expectedActiveRuns {
			testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM flows_flowrun WHERE status = 'W' AND flow_id = $1`, []interface{}{flowID}, activeRuns, "active runs mismatch for flow #%d in '%s'", flowID, tc.label)
		}
	}
}
