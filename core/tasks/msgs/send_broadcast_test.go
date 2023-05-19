package msgs_test

import (
	"testing"
	"time"

	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	_ "github.com/nyaruka/mailroom/core/handlers"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/queue"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/core/tasks/msgs"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSendBroadcastTask(t *testing.T) {
	ctx, rt, mocks, close := testsuite.RuntimeWithSearch()
	defer close()

	rc := rt.RP.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetAll)

	oa, err := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
	require.NoError(t, err)

	eng := envs.Language("eng")
	basic := flows.BroadcastTranslations{
		eng: {
			Text:         "hello world",
			Attachments:  nil,
			QuickReplies: nil,
		},
	}

	doctors := assets.NewGroupReference(testdata.DoctorsGroup.UUID, "Doctors")
	doctorsOnly := []*assets.GroupReference{doctors}

	cathy := flows.NewContactReference(testdata.Cathy.UUID, "Cathy")
	cathyOnly := []*flows.ContactReference{cathy}

	// add an extra URN fo cathy
	testdata.InsertContactURN(rt, testdata.Org1, testdata.Cathy, urns.URN("tel:+12065551212"), 1001)

	// change george's URN to an invalid twitter URN so it can't be sent
	rt.DB.MustExec(`UPDATE contacts_contacturn SET identity = 'twitter:invalid-urn', scheme = 'twitter', path='invalid-urn' WHERE id = $1`, testdata.George.URNID)
	george := flows.NewContactReference(testdata.George.UUID, "George")
	georgeOnly := []*flows.ContactReference{george}

	doctorIDs := testdata.DoctorsGroup.ContactIDs(rt)

	tcs := []struct {
		translations       flows.BroadcastTranslations
		baseLanguage       envs.Language
		groups             []*assets.GroupReference
		contacts           []*flows.ContactReference
		urns               []urns.URN
		queue              string
		elasticResult      []models.ContactID
		expectedBatchCount int
		expectedMsgCount   int
		expectedMsgText    string
	}{
		{ // 0
			translations:       basic,
			baseLanguage:       eng,
			groups:             doctorsOnly,
			contacts:           nil,
			urns:               nil,
			queue:              queue.BatchQueue,
			elasticResult:      doctorIDs,
			expectedBatchCount: 2,
			expectedMsgCount:   121,
			expectedMsgText:    "hello world",
		},
		{ // 1
			translations:       basic,
			baseLanguage:       eng,
			groups:             doctorsOnly,
			contacts:           georgeOnly,
			urns:               nil,
			queue:              queue.BatchQueue,
			elasticResult:      append([]models.ContactID{testdata.George.ID}, doctorIDs...),
			expectedBatchCount: 2,
			expectedMsgCount:   122,
			expectedMsgText:    "hello world",
		},
		{ // 2
			translations:       basic,
			baseLanguage:       eng,
			groups:             nil,
			contacts:           georgeOnly,
			urns:               nil,
			queue:              queue.HandlerQueue,
			elasticResult:      []models.ContactID{testdata.George.ID},
			expectedBatchCount: 1,
			expectedMsgCount:   1,
			expectedMsgText:    "hello world",
		},
		{ // 3
			translations:       basic,
			baseLanguage:       eng,
			groups:             doctorsOnly,
			contacts:           cathyOnly,
			urns:               nil,
			queue:              queue.BatchQueue,
			elasticResult:      append([]models.ContactID{testdata.Cathy.ID}, doctorIDs...),
			expectedBatchCount: 2,
			expectedMsgCount:   121,
			expectedMsgText:    "hello world",
		},
		{ // 4
			translations:       basic,
			baseLanguage:       eng,
			groups:             nil,
			contacts:           cathyOnly,
			urns:               nil,
			queue:              queue.HandlerQueue,
			elasticResult:      []models.ContactID{testdata.Cathy.ID},
			expectedBatchCount: 1,
			expectedMsgCount:   1,
			expectedMsgText:    "hello world",
		},
		{ // 5
			translations:       basic,
			baseLanguage:       eng,
			groups:             nil,
			contacts:           cathyOnly,
			urns:               []urns.URN{urns.URN("tel:+12065551212")},
			queue:              queue.HandlerQueue,
			elasticResult:      []models.ContactID{testdata.Cathy.ID},
			expectedBatchCount: 1,
			expectedMsgCount:   1,
			expectedMsgText:    "hello world",
		},
		{ // 6
			translations:       basic,
			baseLanguage:       eng,
			groups:             nil,
			contacts:           cathyOnly,
			urns:               []urns.URN{urns.URN("tel:+250700000001")},
			queue:              queue.HandlerQueue,
			elasticResult:      []models.ContactID{testdata.Cathy.ID},
			expectedBatchCount: 1,
			expectedMsgCount:   2,
			expectedMsgText:    "hello world",
		},
		{ // 7
			translations:       basic,
			baseLanguage:       eng,
			groups:             nil,
			contacts:           nil,
			urns:               []urns.URN{urns.URN("tel:+250700000001")},
			queue:              queue.HandlerQueue,
			elasticResult:      []models.ContactID{doctorIDs[0]},
			expectedBatchCount: 1,
			expectedMsgCount:   1,
			expectedMsgText:    "hello world",
		},
	}

	lastNow := time.Now()
	time.Sleep(10 * time.Millisecond)

	for i, tc := range tcs {
		mocks.ES.AddResponse(tc.elasticResult...)

		// handle our start task
		event := events.NewBroadcastCreated(tc.translations, tc.baseLanguage, tc.groups, tc.contacts, "", tc.urns)
		bcast, err := models.NewBroadcastFromEvent(ctx, rt.DB, oa, event)
		assert.NoError(t, err)

		err = tasks.Queue(rc, tc.queue, testdata.Org1.ID, &msgs.SendBroadcastTask{Broadcast: bcast}, queue.DefaultPriority)
		assert.NoError(t, err)

		taskCounts := testsuite.FlushTasks(t, rt)

		// assert our count of batches
		assert.Equal(t, tc.expectedBatchCount, taskCounts["send_broadcast_batch"], "%d: unexpected batch count", i)

		// assert our count of total msgs created
		assertdb.Query(t, rt.DB, `SELECT count(*) FROM msgs_msg WHERE org_id = 1 AND created_on > $1 AND text = $2`, lastNow, tc.expectedMsgText).
			Returns(tc.expectedMsgCount, "%d: unexpected msg count", i)

		lastNow = time.Now()
		time.Sleep(10 * time.Millisecond)
	}
}
