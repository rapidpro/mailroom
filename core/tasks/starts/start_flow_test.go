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

func TestStartTasks(t *testing.T) {
	ctx, rt, mocks, close := testsuite.RuntimeWithSearch()
	defer close()

	rc := rt.RP.Get()
	defer rc.Close()
	defer testsuite.Reset(testsuite.ResetAll)

	// convert our single message flow to an actual background flow that shouldn't interrupt
	rt.DB.MustExec(`UPDATE flows_flow SET flow_type = 'B' WHERE id = $1`, testdata.SingleMessage.ID)

	sID := testdata.InsertWaitingSession(rt, testdata.Org1, testdata.George, models.FlowTypeMessaging, testdata.Favorites, models.NilCallID, time.Now(), time.Now(), true, nil)
	testdata.InsertFlowRun(rt, testdata.Org1, sID, testdata.George, testdata.Favorites, models.RunStatusWaiting)

	doctorIDs := testdata.DoctorsGroup.ContactIDs(rt)

	tcs := []struct {
		label                    string
		flow                     *testdata.Flow
		groupIDs                 []models.GroupID
		excludeGroupIDs          []models.GroupID
		contactIDs               []models.ContactID
		createContact            bool
		query                    string
		excludeInAFlow           bool
		excludeStartedPreviously bool
		queue                    string
		elasticResult            []models.ContactID
		expectedContactCount     int
		expectedBatchCount       int
		expectedTotalCount       int
		expectedStatus           models.StartStatus
		expectedActiveRuns       map[models.FlowID]int
	}{
		{
			label:                    "Empty flow start",
			flow:                     testdata.Favorites,
			excludeInAFlow:           true,
			excludeStartedPreviously: true,
			queue:                    queue.BatchQueue,
			expectedContactCount:     0,
			expectedBatchCount:       0,
			expectedTotalCount:       0,
			expectedStatus:           models.StartStatusComplete,
			expectedActiveRuns:       map[models.FlowID]int{testdata.Favorites.ID: 1, testdata.PickANumber.ID: 0, testdata.SingleMessage.ID: 0},
		},
		{
			label:                    "Single group",
			flow:                     testdata.Favorites,
			groupIDs:                 []models.GroupID{testdata.DoctorsGroup.ID},
			excludeInAFlow:           true,
			excludeStartedPreviously: true,
			queue:                    queue.BatchQueue,
			elasticResult:            doctorIDs,
			expectedContactCount:     121,
			expectedBatchCount:       2,
			expectedTotalCount:       121,
			expectedStatus:           models.StartStatusComplete,
			expectedActiveRuns:       map[models.FlowID]int{testdata.Favorites.ID: 122, testdata.PickANumber.ID: 0, testdata.SingleMessage.ID: 0},
		},
		{
			label:                    "Group and Contact (but all already active)",
			flow:                     testdata.Favorites,
			groupIDs:                 []models.GroupID{testdata.DoctorsGroup.ID},
			contactIDs:               []models.ContactID{testdata.Cathy.ID},
			excludeInAFlow:           true,
			excludeStartedPreviously: true,
			queue:                    queue.BatchQueue,
			elasticResult:            []models.ContactID{},
			expectedContactCount:     0,
			expectedBatchCount:       0,
			expectedTotalCount:       0,
			expectedStatus:           models.StartStatusComplete,
			expectedActiveRuns:       map[models.FlowID]int{testdata.Favorites.ID: 122, testdata.PickANumber.ID: 0, testdata.SingleMessage.ID: 0},
		},
		{
			label:                    "Contact restart",
			flow:                     testdata.Favorites,
			contactIDs:               []models.ContactID{testdata.Cathy.ID},
			excludeInAFlow:           false,
			excludeStartedPreviously: false,
			queue:                    queue.HandlerQueue,
			elasticResult:            []models.ContactID{testdata.Cathy.ID},
			expectedContactCount:     1,
			expectedBatchCount:       1,
			expectedTotalCount:       1,
			expectedStatus:           models.StartStatusComplete,
			expectedActiveRuns:       map[models.FlowID]int{testdata.Favorites.ID: 122, testdata.PickANumber.ID: 0, testdata.SingleMessage.ID: 0},
		},
		{
			label:                    "Previous group and one new contact",
			flow:                     testdata.Favorites,
			groupIDs:                 []models.GroupID{testdata.DoctorsGroup.ID},
			contactIDs:               []models.ContactID{testdata.Bob.ID},
			excludeInAFlow:           false,
			excludeStartedPreviously: true,
			queue:                    queue.BatchQueue,
			elasticResult:            append([]models.ContactID{testdata.Bob.ID}, doctorIDs...),
			expectedContactCount:     122,
			expectedBatchCount:       2,
			expectedTotalCount:       1,
			expectedStatus:           models.StartStatusComplete,
			expectedActiveRuns:       map[models.FlowID]int{testdata.Favorites.ID: 123, testdata.PickANumber.ID: 0, testdata.SingleMessage.ID: 0},
		},
		{
			label:                    "Single contact, no restart",
			flow:                     testdata.Favorites,
			contactIDs:               []models.ContactID{testdata.Bob.ID},
			excludeInAFlow:           false,
			excludeStartedPreviously: true,
			queue:                    queue.HandlerQueue,
			elasticResult:            []models.ContactID{},
			expectedContactCount:     0,
			expectedBatchCount:       0,
			expectedTotalCount:       0,
			expectedStatus:           models.StartStatusComplete,
			expectedActiveRuns:       map[models.FlowID]int{testdata.Favorites.ID: 123, testdata.PickANumber.ID: 0, testdata.SingleMessage.ID: 0},
		},
		{
			label:                    "Single contact, include active, but no restart",
			flow:                     testdata.Favorites,
			contactIDs:               []models.ContactID{testdata.Bob.ID},
			excludeInAFlow:           false,
			excludeStartedPreviously: true,
			queue:                    queue.HandlerQueue,
			elasticResult:            []models.ContactID{},
			expectedContactCount:     0,
			expectedBatchCount:       0,
			expectedTotalCount:       0,
			expectedStatus:           models.StartStatusComplete,
			expectedActiveRuns:       map[models.FlowID]int{testdata.Favorites.ID: 123, testdata.PickANumber.ID: 0, testdata.SingleMessage.ID: 0},
		},
		{
			label:                    "Single contact, include active and restart",
			flow:                     testdata.Favorites,
			contactIDs:               []models.ContactID{testdata.Bob.ID},
			excludeInAFlow:           false,
			excludeStartedPreviously: false,
			queue:                    queue.HandlerQueue,
			elasticResult:            []models.ContactID{testdata.Bob.ID},
			expectedContactCount:     1,
			expectedBatchCount:       1,
			expectedTotalCount:       1,
			expectedStatus:           models.StartStatusComplete,
			expectedActiveRuns:       map[models.FlowID]int{testdata.Favorites.ID: 123, testdata.PickANumber.ID: 0, testdata.SingleMessage.ID: 0},
		},
		{
			label:                    "Query start",
			flow:                     testdata.Favorites,
			query:                    "bob",
			excludeInAFlow:           false,
			excludeStartedPreviously: false,
			queue:                    queue.HandlerQueue,
			elasticResult:            []models.ContactID{testdata.Bob.ID},
			expectedContactCount:     1,
			expectedBatchCount:       1,
			expectedTotalCount:       1,
			expectedStatus:           models.StartStatusComplete,
			expectedActiveRuns:       map[models.FlowID]int{testdata.Favorites.ID: 123, testdata.PickANumber.ID: 0, testdata.SingleMessage.ID: 0},
		},
		{
			label:                    "Query start with invalid query",
			flow:                     testdata.Favorites,
			query:                    "xyz = 45",
			excludeInAFlow:           false,
			excludeStartedPreviously: false,
			queue:                    queue.HandlerQueue,
			elasticResult:            []models.ContactID{},
			expectedContactCount:     0,
			expectedBatchCount:       0,
			expectedTotalCount:       0,
			expectedStatus:           models.StartStatusFailed,
			expectedActiveRuns:       map[models.FlowID]int{testdata.Favorites.ID: 123, testdata.PickANumber.ID: 0, testdata.SingleMessage.ID: 0},
		},
		{
			label:                "New Contact",
			flow:                 testdata.Favorites,
			createContact:        true,
			queue:                queue.HandlerQueue,
			expectedContactCount: 1,
			expectedBatchCount:   1,
			expectedTotalCount:   1,
			expectedStatus:       models.StartStatusComplete,
			expectedActiveRuns:   map[models.FlowID]int{testdata.Favorites.ID: 124, testdata.PickANumber.ID: 0, testdata.SingleMessage.ID: 0},
		},
		{
			label:                    "Other messaging flow",
			flow:                     testdata.PickANumber,
			contactIDs:               []models.ContactID{testdata.Bob.ID},
			excludeInAFlow:           false,
			excludeStartedPreviously: true,
			queue:                    queue.HandlerQueue,
			elasticResult:            []models.ContactID{testdata.Bob.ID},
			expectedContactCount:     1,
			expectedBatchCount:       1,
			expectedTotalCount:       1,
			expectedStatus:           models.StartStatusComplete,
			expectedActiveRuns:       map[models.FlowID]int{testdata.Favorites.ID: 123, testdata.PickANumber.ID: 1, testdata.SingleMessage.ID: 0},
		},
		/*{
			label:                    "Background flow",
			flow:                     testdata.SingleMessage,
			contactIDs:               []models.ContactID{testdata.Bob.ID},
			excludeInAFlow:           false,
			excludeStartedPreviously: true,
			queue:                    queue.HandlerQueue,
			elasticResult:            []models.ContactID{testdata.Bob.ID},
			expectedContactCount:     1,
			expectedBatchCount:       1,
			expectedTotalCount:       1,
			expectedStatus:           models.StartStatusComplete,
			expectedActiveRuns:       map[models.FlowID]int{testdata.Favorites.ID: 123, testdata.PickANumber.ID: 1, testdata.SingleMessage.ID: 0},
		},
		{
			label:                    "Exclude group",
			flow:                     testdata.Favorites,
			contactIDs:               []models.ContactID{testdata.Cathy.ID, testdata.Bob.ID},
			excludeGroupIDs:          []models.GroupID{testdata.DoctorsGroup.ID}, // should exclude Cathy
			excludeInAFlow:           false,
			excludeStartedPreviously: false,
			queue:                    queue.HandlerQueue,
			elasticResult:            []models.ContactID{},
			expectedContactCount:     1,
			expectedBatchCount:       1,
			expectedTotalCount:       1,
			expectedStatus:           models.StartStatusComplete,
			expectedActiveRuns:       map[models.FlowID]int{testdata.Favorites.ID: 124, testdata.PickANumber.ID: 0, testdata.SingleMessage.ID: 0},
		},*/
	}

	for _, tc := range tcs {
		if tc.elasticResult != nil {
			mocks.ES.AddResponse(tc.elasticResult...)
		}

		// handle our start task
		start := models.NewFlowStart(testdata.Org1.ID, models.StartTypeManual, models.FlowTypeMessaging, tc.flow.ID).
			WithGroupIDs(tc.groupIDs).
			WithContactIDs(tc.contactIDs).
			WithQuery(tc.query).
			WithExcludeInAFlow(tc.excludeInAFlow).
			WithExcludeStartedPreviously(tc.excludeStartedPreviously).
			WithExcludeGroupIDs(tc.excludeGroupIDs).
			WithCreateContact(tc.createContact)

		err := models.InsertFlowStarts(ctx, rt.DB, []*models.FlowStart{start})
		assert.NoError(t, err)

		err = tasks.Queue(rc, tc.queue, testdata.Org1.ID, &starts.StartFlowTask{FlowStart: start}, queue.DefaultPriority)
		assert.NoError(t, err)

		taskCounts := testsuite.FlushTasks(t, rt)

		// assert our count of batches
		assert.Equal(t, tc.expectedBatchCount, taskCounts["start_flow_batch"], "unexpected batch count in '%s'", tc.label)

		// assert our count of total flow runs created
		assertdb.Query(t, rt.DB, `SELECT count(*) FROM flows_flowrun WHERE flow_id = $1 AND start_id = $2`, tc.flow.ID, start.ID).Returns(tc.expectedTotalCount, "unexpected total run count in '%s'", tc.label)

		// assert final status
		assertdb.Query(t, rt.DB, `SELECT count(*) FROM flows_flowstart where status = $2 AND id = $1`, start.ID, tc.expectedStatus).Returns(1, "status mismatch in '%s'", tc.label)

		// assert final contact count
		if tc.expectedStatus != models.StartStatusFailed {
			assertdb.Query(t, rt.DB, `SELECT contact_count FROM flows_flowstart where id = $1`, start.ID).Returns(tc.expectedContactCount, "contact count mismatch in '%s'", tc.label)
		}

		// assert count of active runs by flow
		for flowID, activeRuns := range tc.expectedActiveRuns {
			assertdb.Query(t, rt.DB, `SELECT count(*) FROM flows_flowrun WHERE status = 'W' AND flow_id = $1`, flowID).Returns(activeRuns, "active runs mismatch for flow #%d in '%s'", flowID, tc.label)
		}
	}
}
