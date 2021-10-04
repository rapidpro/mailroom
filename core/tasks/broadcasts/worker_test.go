package broadcasts

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	_ "github.com/nyaruka/mailroom/core/handlers"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/queue"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"

	"github.com/stretchr/testify/assert"
)

func TestBroadcastEvents(t *testing.T) {
	ctx, rt, db, rp := testsuite.Get()
	rc := rp.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetAll)

	oa, err := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
	assert.NoError(t, err)

	eng := envs.Language("eng")
	basic := map[envs.Language]*events.BroadcastTranslation{
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
	testdata.InsertContactURN(db, testdata.Org1, testdata.Cathy, urns.URN("tel:+12065551212"), 1001)

	// change george's URN to an invalid twitter URN so it can't be sent
	db.MustExec(
		`UPDATE contacts_contacturn SET identity = 'twitter:invalid-urn', scheme = 'twitter', path='invalid-urn' WHERE id = $1`, testdata.George.URNID,
	)
	george := flows.NewContactReference(testdata.George.UUID, "George")
	georgeOnly := []*flows.ContactReference{george}

	tcs := []struct {
		Translations map[envs.Language]*events.BroadcastTranslation
		BaseLanguage envs.Language
		Groups       []*assets.GroupReference
		Contacts     []*flows.ContactReference
		URNs         []urns.URN
		Queue        string
		BatchCount   int
		MsgCount     int
		MsgText      string
	}{
		{basic, eng, doctorsOnly, nil, nil, queue.BatchQueue, 2, 121, "hello world"},
		{basic, eng, doctorsOnly, georgeOnly, nil, queue.BatchQueue, 2, 121, "hello world"},
		{basic, eng, nil, georgeOnly, nil, queue.HandlerQueue, 1, 0, "hello world"},
		{basic, eng, doctorsOnly, cathyOnly, nil, queue.BatchQueue, 2, 121, "hello world"},
		{basic, eng, nil, cathyOnly, nil, queue.HandlerQueue, 1, 1, "hello world"},
		{basic, eng, nil, cathyOnly, []urns.URN{urns.URN("tel:+12065551212")}, queue.HandlerQueue, 1, 1, "hello world"},
		{basic, eng, nil, cathyOnly, []urns.URN{urns.URN("tel:+250700000001")}, queue.HandlerQueue, 1, 2, "hello world"},
		{basic, eng, nil, nil, []urns.URN{urns.URN("tel:+250700000001")}, queue.HandlerQueue, 1, 1, "hello world"},
	}

	lastNow := time.Now()
	time.Sleep(10 * time.Millisecond)

	for i, tc := range tcs {
		// handle our start task
		event := events.NewBroadcastCreated(tc.Translations, tc.BaseLanguage, tc.Groups, tc.Contacts, tc.URNs)
		bcast, err := models.NewBroadcastFromEvent(ctx, db, oa, event)
		assert.NoError(t, err)

		err = CreateBroadcastBatches(ctx, rt, bcast)
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
			assert.Equal(t, queue.SendBroadcastBatch, task.Type)
			batch := &models.BroadcastBatch{}
			err = json.Unmarshal(task.Task, batch)
			assert.NoError(t, err)

			err = SendBroadcastBatch(ctx, rt, batch)
			assert.NoError(t, err)
		}

		// assert our count of batches
		assert.Equal(t, tc.BatchCount, count, "%d: unexpected batch count", i)

		// assert our count of total msgs created
		testsuite.AssertQuery(t, db, `SELECT count(*) FROM msgs_msg WHERE org_id = 1 AND created_on > $1 AND topup_id IS NOT NULL AND text = $2`, lastNow, tc.MsgText).
			Returns(tc.MsgCount, "%d: unexpected msg count", i)

		lastNow = time.Now()
		time.Sleep(10 * time.Millisecond)
	}
}

func TestBroadcastTask(t *testing.T) {
	ctx, rt, db, rp := testsuite.Get()
	rc := rp.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetAll)

	oa, err := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
	assert.NoError(t, err)
	eng := envs.Language("eng")

	// insert a broadcast so we can check it is being set to sent
	legacyID := testdata.InsertBroadcast(db, testdata.Org1, "base", map[envs.Language]string{"base": "hi @(PROPER(contact.name)) legacy"}, models.NilScheduleID, nil, nil)

	ticket := testdata.InsertOpenTicket(db, testdata.Org1, testdata.Cathy, testdata.Mailgun, testdata.DefaultTopic, "", "", nil)
	modelTicket := ticket.Load(db)

	evaluated := map[envs.Language]*models.BroadcastTranslation{
		eng: {
			Text:         "hello world",
			Attachments:  nil,
			QuickReplies: nil,
		},
	}

	legacy := map[envs.Language]*models.BroadcastTranslation{
		eng: {
			Text:         "hi @(PROPER(contact.name)) legacy URN: @contact.tel_e164 Gender: @contact.gender",
			Attachments:  nil,
			QuickReplies: nil,
		},
	}

	template := map[envs.Language]*models.BroadcastTranslation{
		eng: {
			Text:         "hi @(title(contact.name)) from @globals.org_name goflow URN: @urns.tel Gender: @fields.gender",
			Attachments:  nil,
			QuickReplies: nil,
		},
	}

	doctorsOnly := []models.GroupID{testdata.DoctorsGroup.ID}
	cathyOnly := []models.ContactID{testdata.Cathy.ID}

	// add an extra URN fo cathy
	testdata.InsertContactURN(db, testdata.Org1, testdata.Cathy, urns.URN("tel:+12065551212"), 1001)

	tcs := []struct {
		BroadcastID   models.BroadcastID
		Translations  map[envs.Language]*models.BroadcastTranslation
		TemplateState models.TemplateState
		BaseLanguage  envs.Language
		GroupIDs      []models.GroupID
		ContactIDs    []models.ContactID
		URNs          []urns.URN
		TicketID      models.TicketID
		Queue         string
		BatchCount    int
		MsgCount      int
		MsgText       string
	}{
		{
			models.NilBroadcastID,
			evaluated,
			models.TemplateStateEvaluated,
			eng,
			doctorsOnly,
			cathyOnly,
			nil,
			ticket.ID,
			queue.BatchQueue,
			2,
			121,
			"hello world",
		},
		{
			legacyID,
			legacy,
			models.TemplateStateLegacy,
			eng,
			nil,
			cathyOnly,
			nil,
			models.NilTicketID,
			queue.HandlerQueue,
			1,
			1,
			"hi Cathy legacy URN: +12065551212 Gender: F",
		},
		{
			models.NilBroadcastID,
			template,
			models.TemplateStateUnevaluated,
			eng,
			nil,
			cathyOnly,
			nil,
			models.NilTicketID,
			queue.HandlerQueue,
			1,
			1,
			"hi Cathy from Nyaruka goflow URN: tel:+12065551212 Gender: F",
		},
	}

	lastNow := time.Now()
	time.Sleep(10 * time.Millisecond)

	for i, tc := range tcs {
		// handle our start task
		bcast := models.NewBroadcast(oa.OrgID(), tc.BroadcastID, tc.Translations, tc.TemplateState, tc.BaseLanguage, tc.URNs, tc.ContactIDs, tc.GroupIDs, tc.TicketID)
		err = CreateBroadcastBatches(ctx, rt, bcast)
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
			assert.Equal(t, queue.SendBroadcastBatch, task.Type)
			batch := &models.BroadcastBatch{}
			err = json.Unmarshal(task.Task, batch)
			assert.NoError(t, err)

			err = SendBroadcastBatch(ctx, rt, batch)
			assert.NoError(t, err)
		}

		// assert our count of batches
		assert.Equal(t, tc.BatchCount, count, "%d: unexpected batch count", i)

		// assert our count of total msgs created
		testsuite.AssertQuery(t, db, `SELECT count(*) FROM msgs_msg WHERE org_id = 1 AND created_on > $1 AND topup_id IS NOT NULL AND text = $2`, lastNow, tc.MsgText).
			Returns(tc.MsgCount, "%d: unexpected msg count", i)

		// make sure our broadcast is marked as sent
		if tc.BroadcastID != models.NilBroadcastID {
			testsuite.AssertQuery(t, db, `SELECT count(*) FROM msgs_broadcast WHERE id = $1 AND status = 'S'`, tc.BroadcastID).
				Returns(1, "%d: broadcast not marked as sent", i)
		}

		// if we had a ticket, make sure its last_activity_on was updated
		if tc.TicketID != models.NilTicketID {
			testsuite.AssertQuery(t, db, `SELECT count(*) FROM tickets_ticket WHERE id = $1 AND last_activity_on > $2`, tc.TicketID, modelTicket.LastActivityOn()).
				Returns(1, "%d: ticket last_activity_on not updated", i)
		}

		lastNow = time.Now()
		time.Sleep(10 * time.Millisecond)
	}
}
