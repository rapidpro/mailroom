package starts

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/config"
	_ "github.com/nyaruka/mailroom/hooks"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/queue"
	"github.com/nyaruka/mailroom/runner"
	"github.com/nyaruka/mailroom/testsuite"

	"github.com/olivere/elastic"
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

	// insert a flow run for one of our contacts
	// TODO: can be replaced with a normal flow start of another flow once we support flows with waits
	db.MustExec(
		`INSERT INTO flows_flowrun(uuid, status, is_active, created_on, modified_on, responded, contact_id, flow_id, org_id)
		                    VALUES($1, 'W', TRUE, now(), now(), FALSE, $2, $3, 1);`, uuids.New(), models.GeorgeID, models.SingleMessageFlowID)

	tcs := []struct {
		Label               string
		FlowID              models.FlowID
		GroupIDs            []models.GroupID
		ContactIDs          []models.ContactID
		CreateContact       bool
		Query               string
		QueryResponse       string
		RestartParticipants models.RestartParticipants
		IncludeActive       models.IncludeActive
		Queue               string
		ContactCount        int
		BatchCount          int
		TotalCount          int
		Status              models.StartStatus
	}{
		{
			Label:        "empty flow start",
			FlowID:       models.SingleMessageFlowID,
			Queue:        queue.BatchQueue,
			ContactCount: 0,
			BatchCount:   0,
			TotalCount:   0,
			Status:       models.StartStatusComplete,
		},
		{
			Label:        "Single group",
			FlowID:       models.SingleMessageFlowID,
			GroupIDs:     []models.GroupID{models.DoctorsGroupID},
			Queue:        queue.BatchQueue,
			ContactCount: 121,
			BatchCount:   2,
			TotalCount:   121,
			Status:       models.StartStatusComplete,
		},
		{
			Label:        "Group and Contact (but all already active)",
			FlowID:       models.SingleMessageFlowID,
			GroupIDs:     []models.GroupID{models.DoctorsGroupID},
			ContactIDs:   []models.ContactID{models.CathyID},
			Queue:        queue.BatchQueue,
			ContactCount: 121,
			BatchCount:   2,
			TotalCount:   0,
			Status:       models.StartStatusComplete,
		},
		{
			Label:               "Contact restart",
			FlowID:              models.SingleMessageFlowID,
			ContactIDs:          []models.ContactID{models.CathyID},
			RestartParticipants: true,
			IncludeActive:       true,
			Queue:               queue.HandlerQueue,
			ContactCount:        1,
			BatchCount:          1,
			TotalCount:          1,
			Status:              models.StartStatusComplete,
		},
		{
			Label:        "Previous group and one new contact",
			FlowID:       models.SingleMessageFlowID,
			GroupIDs:     []models.GroupID{models.DoctorsGroupID},
			ContactIDs:   []models.ContactID{models.BobID},
			Queue:        queue.BatchQueue,
			ContactCount: 122,
			BatchCount:   2,
			TotalCount:   1,
			Status:       models.StartStatusComplete,
		},
		{
			Label:        "Single contact, no restart",
			FlowID:       models.SingleMessageFlowID,
			ContactIDs:   []models.ContactID{models.BobID},
			Queue:        queue.HandlerQueue,
			ContactCount: 1,
			BatchCount:   1,
			TotalCount:   0,
			Status:       models.StartStatusComplete,
		},
		{
			Label:         "Single contact, include active, but no restart",
			FlowID:        models.SingleMessageFlowID,
			ContactIDs:    []models.ContactID{models.BobID},
			IncludeActive: true,
			Queue:         queue.HandlerQueue,
			ContactCount:  1,
			BatchCount:    1,
			TotalCount:    0,
			Status:        models.StartStatusComplete,
		},
		{
			Label:               "Single contact, include active and restart",
			FlowID:              models.SingleMessageFlowID,
			ContactIDs:          []models.ContactID{models.BobID},
			RestartParticipants: true,
			IncludeActive:       true,
			Queue:               queue.HandlerQueue,
			ContactCount:        1,
			BatchCount:          1,
			TotalCount:          1,
			Status:              models.StartStatusComplete,
		},
		{
			Label:  "Query start",
			FlowID: models.SingleMessageFlowID,
			Query:  "bob",
			QueryResponse: fmt.Sprintf(`{
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
			RestartParticipants: true,
			IncludeActive:       true,
			Queue:               queue.HandlerQueue,
			ContactCount:        1,
			BatchCount:          1,
			TotalCount:          1,
			Status:              models.StartStatusComplete,
		},
		{
			Label:               "Query start with invalid query",
			FlowID:              models.SingleMessageFlowID,
			Query:               "xyz = 45",
			RestartParticipants: true,
			IncludeActive:       true,
			Queue:               queue.HandlerQueue,
			ContactCount:        0,
			BatchCount:          0,
			TotalCount:          0,
			Status:              models.StartStatusFailed,
		},
		{
			Label:         "New Contact",
			FlowID:        models.SingleMessageFlowID,
			CreateContact: true,
			Queue:         queue.HandlerQueue,
			ContactCount:  1,
			BatchCount:    1,
			TotalCount:    1,
			Status:        models.StartStatusComplete,
		},
	}

	for _, tc := range tcs {
		mes.NextResponse = tc.QueryResponse

		// handle our start task
		start := models.NewFlowStart(models.Org1, models.StartTypeManual, models.MessagingFlow, tc.FlowID, tc.RestartParticipants, tc.IncludeActive).
			WithGroupIDs(tc.GroupIDs).
			WithContactIDs(tc.ContactIDs).
			WithQuery(tc.Query).
			WithCreateContact(tc.CreateContact)

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
			task, err = queue.PopNextTask(rc, tc.Queue)
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
		assert.Equal(t, tc.BatchCount, count, "unexpected batch count in '%s'", tc.Label)

		// assert our count of total flow runs created
		testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM flows_flowrun where flow_id = $1 AND start_id = $2 AND is_active = FALSE`,
			[]interface{}{tc.FlowID, start.ID()}, tc.TotalCount, "unexpected total run count in '%s'", tc.Label)

		// assert final status
		testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM flows_flowstart where status = $2 AND id = $1`,
			[]interface{}{start.ID(), tc.Status}, 1, "status mismatch in '%s'", tc.Label)

		// assert final contact count
		if tc.Status != models.StartStatusFailed {
			testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM flows_flowstart where contact_count = $2 AND id = $1`,
				[]interface{}{start.ID(), tc.ContactCount}, 1, "contact count mismatch in '%s'", tc.Label)
		}
	}
}
