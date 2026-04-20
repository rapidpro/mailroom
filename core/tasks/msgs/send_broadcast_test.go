package msgs_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/gocommon/i18n"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/assets"
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
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetAll)

	rc := rt.RP.Get()
	defer rc.Close()

	oa, err := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
	require.NoError(t, err)

	eng := i18n.Language("eng")
	basic := flows.BroadcastTranslations{
		eng: {
			Text:         "hello world",
			Attachments:  nil,
			QuickReplies: nil,
		},
	}

	doctors := assets.NewGroupReference(testdata.DoctorsGroup.UUID, "Doctors")
	cathy := flows.NewContactReference(testdata.Cathy.UUID, "Cathy")

	// add an extra URN fo cathy
	testdata.InsertContactURN(rt, testdata.Org1, testdata.Cathy, urns.URN("tel:+12065551212"), 1001, nil)

	// change george's URN to an invalid twitter URN so it can't be sent
	rt.DB.MustExec(`UPDATE contacts_contacturn SET identity = 'twitter:invalid-urn', scheme = 'twitter', path='invalid-urn' WHERE id = $1`, testdata.George.URNID)
	george := flows.NewContactReference(testdata.George.UUID, "George")
	georgeOnly := []*flows.ContactReference{george}

	tcs := []struct {
		translations       flows.BroadcastTranslations
		baseLanguage       i18n.Language
		groups             []*assets.GroupReference
		contacts           []*flows.ContactReference
		urns               []urns.URN
		queue              string
		expectedBatchCount int
		expectedMsgCount   int
		expectedMsgText    string
	}{
		{ // 0
			translations:       basic,
			baseLanguage:       eng,
			groups:             []*assets.GroupReference{doctors},
			contacts:           nil,
			urns:               nil,
			queue:              queue.BatchQueue,
			expectedBatchCount: 2,
			expectedMsgCount:   121,
			expectedMsgText:    "hello world",
		},
		{ // 1
			translations:       basic,
			baseLanguage:       eng,
			groups:             []*assets.GroupReference{doctors},
			contacts:           georgeOnly,
			urns:               nil,
			queue:              queue.BatchQueue,
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
			expectedBatchCount: 1,
			expectedMsgCount:   1,
			expectedMsgText:    "hello world",
		},
		{ // 3
			translations:       basic,
			baseLanguage:       eng,
			groups:             []*assets.GroupReference{doctors},
			contacts:           []*flows.ContactReference{cathy},
			urns:               nil,
			queue:              queue.BatchQueue,
			expectedBatchCount: 2,
			expectedMsgCount:   121,
			expectedMsgText:    "hello world",
		},
		{ // 4
			translations:       basic,
			baseLanguage:       eng,
			groups:             nil,
			contacts:           []*flows.ContactReference{cathy},
			urns:               nil,
			queue:              queue.HandlerQueue,
			expectedBatchCount: 1,
			expectedMsgCount:   1,
			expectedMsgText:    "hello world",
		},
		{ // 5
			translations:       basic,
			baseLanguage:       eng,
			groups:             nil,
			contacts:           []*flows.ContactReference{cathy},
			urns:               []urns.URN{urns.URN("tel:+12065551212")},
			queue:              queue.HandlerQueue,
			expectedBatchCount: 1,
			expectedMsgCount:   1,
			expectedMsgText:    "hello world",
		},
		{ // 6
			translations:       basic,
			baseLanguage:       eng,
			groups:             nil,
			contacts:           []*flows.ContactReference{cathy},
			urns:               []urns.URN{urns.URN("tel:+250700000001")},
			queue:              queue.HandlerQueue,
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
			expectedBatchCount: 1,
			expectedMsgCount:   1,
			expectedMsgText:    "hello world",
		},
	}

	lastNow := time.Now()
	time.Sleep(10 * time.Millisecond)

	for i, tc := range tcs {
		testsuite.ReindexElastic(ctx)

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

func TestBroadcastTask(t *testing.T) {
	ctx, rt := testsuite.Runtime()
	rc := rt.RP.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetAll)

	polls := testdata.InsertOptIn(rt, testdata.Org1, "Polls")

	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshOptIns)
	assert.NoError(t, err)
	eng := i18n.Language("eng")

	doctorsOnly := []models.GroupID{testdata.DoctorsGroup.ID}
	cathyOnly := []models.ContactID{testdata.Cathy.ID}

	// add an extra URN fo cathy
	testdata.InsertContactURN(rt, testdata.Org1, testdata.Cathy, urns.URN("tel:+12065551212"), 1001, nil)

	tcs := []struct {
		translations     flows.BroadcastTranslations
		templateState    models.TemplateState
		baseLanguage     i18n.Language
		optIn            *testdata.OptIn
		groupIDs         []models.GroupID
		contactIDs       []models.ContactID
		URNs             []urns.URN
		createdByID      models.UserID
		queue            string
		expectedBatches  int
		expectedMsgCount int
		expectedMsgText  string
	}{
		{
			flows.BroadcastTranslations{
				eng: {
					Text:         "hello world",
					Attachments:  nil,
					QuickReplies: nil,
				},
			},
			models.TemplateStateEvaluated,
			eng,
			polls,
			doctorsOnly,
			cathyOnly,
			nil,
			testdata.Admin.ID,
			queue.BatchQueue,
			2,
			121,
			"hello world",
		},
		{
			flows.BroadcastTranslations{
				eng: {
					Text:         "hi @(title(contact.name)) from @globals.org_name goflow URN: @urns.tel Gender: @fields.gender",
					Attachments:  nil,
					QuickReplies: nil,
				},
			},
			models.TemplateStateUnevaluated,
			eng,
			nil,
			nil,
			cathyOnly,
			nil,
			testdata.Agent.ID,
			queue.HandlerQueue,
			1,
			1,
			"hi Cathy from Nyaruka goflow URN: tel:+12065551212 Gender: F",
		},
	}

	lastNow := time.Now()
	time.Sleep(10 * time.Millisecond)

	for i, tc := range tcs {
		var optInID models.OptInID
		if tc.optIn != nil {
			optInID = tc.optIn.ID
		}

		bcast := models.NewBroadcast(oa.OrgID(), tc.translations, tc.templateState, tc.baseLanguage, optInID, tc.URNs, tc.contactIDs, tc.groupIDs, "", tc.createdByID)

		err = (&msgs.SendBroadcastTask{Broadcast: bcast}).Perform(ctx, rt, testdata.Org1.ID)
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
			assert.Equal(t, "send_broadcast_batch", task.Type)
			taskObj := &msgs.SendBroadcastBatchTask{}
			err = json.Unmarshal(task.Task, taskObj)
			assert.NoError(t, err)

			err = taskObj.Perform(ctx, rt, testdata.Org1.ID)
			assert.NoError(t, err)
		}

		// assert our count of batches
		assert.Equal(t, tc.expectedBatches, count, "%d: unexpected batch count", i)

		// assert our count of total msgs created
		assertdb.Query(t, rt.DB, `SELECT count(*) FROM msgs_msg WHERE org_id = 1 AND created_on > $1 AND text = $2`, lastNow, tc.expectedMsgText).
			Returns(tc.expectedMsgCount, "%d: unexpected msg count", i)

		if tc.optIn != nil {
			assertdb.Query(t, rt.DB, `SELECT count(*) FROM msgs_msg WHERE org_id = 1 AND created_on > $1 AND optin_id = $2`, lastNow, optInID)
		}

		lastNow = time.Now()
		time.Sleep(10 * time.Millisecond)
	}
}
