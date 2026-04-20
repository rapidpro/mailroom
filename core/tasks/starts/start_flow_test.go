package starts_test

import (
	"testing"
	"time"

	"github.com/nyaruka/gocommon/dbutil/assertdb"
	_ "github.com/nyaruka/mailroom/core/handlers"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/queue"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/core/tasks/starts"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/assert"
)

func TestStartFlowTask(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetAll)

	rc := rt.RP.Get()
	defer rc.Close()

	// convert our single message flow to an actual background flow that shouldn't interrupt
	rt.DB.MustExec(`UPDATE flows_flow SET flow_type = 'B' WHERE id = $1`, testdata.SingleMessage.ID)

	sID := testdata.InsertWaitingSession(rt, testdata.Org1, testdata.George, models.FlowTypeMessaging, testdata.Favorites, models.NilCallID, time.Now(), time.Now(), true, nil)
	testdata.InsertFlowRun(rt, testdata.Org1, sID, testdata.George, testdata.Favorites, models.RunStatusWaiting)

	tcs := []struct {
		flowID                   models.FlowID
		groupIDs                 []models.GroupID
		excludeGroupIDs          []models.GroupID
		contactIDs               []models.ContactID
		createContact            bool
		query                    string
		excludeInAFlow           bool
		excludeStartedPreviously bool
		queue                    string
		expectedContactCount     int
		expectedBatchCount       int
		expectedTotalCount       int
		expectedStatus           models.StartStatus
		expectedActiveRuns       map[models.FlowID]int
	}{
		{ // 0: empty flow start
			flowID:                   testdata.Favorites.ID,
			excludeInAFlow:           true,
			excludeStartedPreviously: true,
			queue:                    queue.BatchQueue,
			expectedContactCount:     0,
			expectedBatchCount:       0,
			expectedTotalCount:       0,
			expectedStatus:           models.StartStatusComplete,
			expectedActiveRuns:       map[models.FlowID]int{testdata.Favorites.ID: 1, testdata.PickANumber.ID: 0, testdata.SingleMessage.ID: 0},
		},
		{ // 1: single group
			flowID:                   testdata.Favorites.ID,
			groupIDs:                 []models.GroupID{testdata.DoctorsGroup.ID},
			excludeInAFlow:           true,
			excludeStartedPreviously: true,
			queue:                    queue.BatchQueue,
			expectedContactCount:     121,
			expectedBatchCount:       2,
			expectedTotalCount:       121,
			expectedStatus:           models.StartStatusComplete,
			expectedActiveRuns:       map[models.FlowID]int{testdata.Favorites.ID: 122, testdata.PickANumber.ID: 0, testdata.SingleMessage.ID: 0},
		},
		{ // 2: group and contact (but all already active)
			flowID:                   testdata.Favorites.ID,
			groupIDs:                 []models.GroupID{testdata.DoctorsGroup.ID},
			contactIDs:               []models.ContactID{testdata.Cathy.ID},
			excludeInAFlow:           true,
			excludeStartedPreviously: true,
			queue:                    queue.BatchQueue,
			expectedContactCount:     121,
			expectedBatchCount:       0,
			expectedTotalCount:       0,
			expectedStatus:           models.StartStatusComplete,
			expectedActiveRuns:       map[models.FlowID]int{testdata.Favorites.ID: 122, testdata.PickANumber.ID: 0, testdata.SingleMessage.ID: 0},
		},
		{ // 3: don't exclude started previously
			flowID:                   testdata.Favorites.ID,
			contactIDs:               []models.ContactID{testdata.Cathy.ID},
			excludeInAFlow:           false,
			excludeStartedPreviously: false,
			queue:                    queue.HandlerQueue,
			expectedContactCount:     1,
			expectedBatchCount:       1,
			expectedTotalCount:       1,
			expectedStatus:           models.StartStatusComplete,
			expectedActiveRuns:       map[models.FlowID]int{testdata.Favorites.ID: 122, testdata.PickANumber.ID: 0, testdata.SingleMessage.ID: 0},
		},
		{ // 4: previous group and one new contact
			flowID:                   testdata.Favorites.ID,
			groupIDs:                 []models.GroupID{testdata.DoctorsGroup.ID},
			contactIDs:               []models.ContactID{testdata.Bob.ID},
			excludeStartedPreviously: true,
			queue:                    queue.BatchQueue,
			expectedContactCount:     122,
			expectedBatchCount:       1,
			expectedTotalCount:       1,
			expectedStatus:           models.StartStatusComplete,
			expectedActiveRuns:       map[models.FlowID]int{testdata.Favorites.ID: 123, testdata.PickANumber.ID: 0, testdata.SingleMessage.ID: 0},
		},
		{ // 5: single contact, no restart
			flowID:                   testdata.Favorites.ID,
			contactIDs:               []models.ContactID{testdata.Bob.ID},
			excludeStartedPreviously: true,
			queue:                    queue.HandlerQueue,
			expectedContactCount:     1,
			expectedBatchCount:       0,
			expectedTotalCount:       0,
			expectedStatus:           models.StartStatusComplete,
			expectedActiveRuns:       map[models.FlowID]int{testdata.Favorites.ID: 123, testdata.PickANumber.ID: 0, testdata.SingleMessage.ID: 0},
		},
		{ // 6: single contact, include active, but no restart
			flowID:                   testdata.Favorites.ID,
			contactIDs:               []models.ContactID{testdata.Bob.ID},
			excludeInAFlow:           false,
			excludeStartedPreviously: true,
			queue:                    queue.HandlerQueue,
			expectedContactCount:     1,
			expectedBatchCount:       0,
			expectedTotalCount:       0,
			expectedStatus:           models.StartStatusComplete,
			expectedActiveRuns:       map[models.FlowID]int{testdata.Favorites.ID: 123, testdata.PickANumber.ID: 0, testdata.SingleMessage.ID: 0},
		},
		{ // 7: single contact, include active and restart
			flowID:                   testdata.Favorites.ID,
			contactIDs:               []models.ContactID{testdata.Bob.ID},
			excludeInAFlow:           false,
			excludeStartedPreviously: false,
			queue:                    queue.HandlerQueue,
			expectedContactCount:     1,
			expectedBatchCount:       1,
			expectedTotalCount:       1,
			expectedStatus:           models.StartStatusComplete,
			expectedActiveRuns:       map[models.FlowID]int{testdata.Favorites.ID: 123, testdata.PickANumber.ID: 0, testdata.SingleMessage.ID: 0},
		},
		{ // 8: query start
			flowID:                   testdata.Favorites.ID,
			query:                    "bob",
			excludeInAFlow:           false,
			excludeStartedPreviously: false,
			queue:                    queue.HandlerQueue,
			expectedContactCount:     1,
			expectedBatchCount:       1,
			expectedTotalCount:       1,
			expectedStatus:           models.StartStatusComplete,
			expectedActiveRuns:       map[models.FlowID]int{testdata.Favorites.ID: 123, testdata.PickANumber.ID: 0, testdata.SingleMessage.ID: 0},
		},
		{ // 9: query start with invalid query
			flowID:                   testdata.Favorites.ID,
			query:                    "xyz = 45",
			excludeInAFlow:           false,
			excludeStartedPreviously: false,
			queue:                    queue.HandlerQueue,
			expectedContactCount:     0,
			expectedBatchCount:       0,
			expectedTotalCount:       0,
			expectedStatus:           models.StartStatusFailed,
			expectedActiveRuns:       map[models.FlowID]int{testdata.Favorites.ID: 123, testdata.PickANumber.ID: 0, testdata.SingleMessage.ID: 0},
		},
		{ // 10: new contact
			flowID:               testdata.Favorites.ID,
			createContact:        true,
			queue:                queue.HandlerQueue,
			expectedContactCount: 1,
			expectedBatchCount:   1,
			expectedTotalCount:   1,
			expectedStatus:       models.StartStatusComplete,
			expectedActiveRuns:   map[models.FlowID]int{testdata.Favorites.ID: 124, testdata.PickANumber.ID: 0, testdata.SingleMessage.ID: 0},
		},
		{ // 11: other messaging flow
			flowID:                   testdata.PickANumber.ID,
			contactIDs:               []models.ContactID{testdata.Bob.ID},
			excludeInAFlow:           false,
			excludeStartedPreviously: true,
			queue:                    queue.HandlerQueue,
			expectedContactCount:     1,
			expectedBatchCount:       1,
			expectedTotalCount:       1,
			expectedStatus:           models.StartStatusComplete,
			expectedActiveRuns:       map[models.FlowID]int{testdata.Favorites.ID: 123, testdata.PickANumber.ID: 1, testdata.SingleMessage.ID: 0},
		},
		{ // 12: background flow
			flowID:                   testdata.SingleMessage.ID,
			contactIDs:               []models.ContactID{testdata.Bob.ID},
			excludeInAFlow:           false,
			excludeStartedPreviously: true,
			queue:                    queue.HandlerQueue,
			expectedContactCount:     1,
			expectedBatchCount:       1,
			expectedTotalCount:       1,
			expectedStatus:           models.StartStatusComplete,
			expectedActiveRuns:       map[models.FlowID]int{testdata.Favorites.ID: 123, testdata.PickANumber.ID: 1, testdata.SingleMessage.ID: 0},
		},
		{ // 13: exclude group
			flowID:                   testdata.Favorites.ID,
			contactIDs:               []models.ContactID{testdata.Cathy.ID, testdata.Bob.ID},
			excludeGroupIDs:          []models.GroupID{testdata.DoctorsGroup.ID}, // should exclude Cathy
			excludeInAFlow:           false,
			excludeStartedPreviously: false,
			queue:                    queue.HandlerQueue,
			expectedContactCount:     1,
			expectedBatchCount:       1,
			expectedTotalCount:       1,
			expectedStatus:           models.StartStatusComplete,
			expectedActiveRuns:       map[models.FlowID]int{testdata.Favorites.ID: 124, testdata.PickANumber.ID: 0, testdata.SingleMessage.ID: 0},
		},
	}

	for i, tc := range tcs {
		testsuite.ReindexElastic(ctx)

		// handle our start task
		start := models.NewFlowStart(testdata.Org1.ID, models.StartTypeManual, tc.flowID).
			WithGroupIDs(tc.groupIDs).
			WithExcludeGroupIDs(tc.excludeGroupIDs).
			WithContactIDs(tc.contactIDs).
			WithQuery(tc.query).
			WithExcludeInAFlow(tc.excludeInAFlow).
			WithExcludeStartedPreviously(tc.excludeStartedPreviously).
			WithCreateContact(tc.createContact)

		err := models.InsertFlowStarts(ctx, rt.DB, []*models.FlowStart{start})
		assert.NoError(t, err)

		err = tasks.Queue(rc, tc.queue, testdata.Org1.ID, &starts.StartFlowTask{FlowStart: start}, queue.DefaultPriority)
		assert.NoError(t, err)

		taskCounts := testsuite.FlushTasks(t, rt)

		// assert our count of batches
		assert.Equal(t, tc.expectedBatchCount, taskCounts["start_flow_batch"], "%d: unexpected batch count", i)

		// assert our count of total flow runs created
		assertdb.Query(t, rt.DB, `SELECT count(*) FROM flows_flowrun WHERE flow_id = $1 AND start_id = $2`, tc.flowID, start.ID).Returns(tc.expectedTotalCount, "%d: unexpected total run count", i)

		// assert final status
		assertdb.Query(t, rt.DB, `SELECT count(*) FROM flows_flowstart where status = $2 AND id = $1`, start.ID, tc.expectedStatus).Returns(1, "%d: status mismatch", i)

		// assert final contact count
		if tc.expectedStatus != models.StartStatusFailed {
			assertdb.Query(t, rt.DB, `SELECT count(*) FROM flows_flowstart where contact_count = $2 AND id = $1`, []any{start.ID, tc.expectedContactCount}, 1, "%d: contact count mismatch", i)
		}

		// assert count of active runs by flow
		for flowID, activeRuns := range tc.expectedActiveRuns {
			assertdb.Query(t, rt.DB, `SELECT count(*) FROM flows_flowrun WHERE status = 'W' AND flow_id = $1`, flowID).Returns(activeRuns, "%d: active runs mismatch for flow #%d", i, flowID)
		}
	}
}
