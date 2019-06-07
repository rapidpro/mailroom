package broadcasts

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/nyaruka/goflow/assets"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/goflow/utils"
	_ "github.com/nyaruka/mailroom/hooks"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/queue"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/stretchr/testify/assert"
)

func TestBroadcasts(t *testing.T) {
	testsuite.Reset()
	ctx := testsuite.CTX()
	rp := testsuite.RP()
	db := testsuite.DB()
	rc := testsuite.RC()
	defer rc.Close()

	org, err := models.GetOrgAssets(ctx, db, models.Org1)
	assert.NoError(t, err)

	eng := utils.Language("eng")
	basic := map[utils.Language]*events.BroadcastTranslation{
		eng: &events.BroadcastTranslation{
			Text:         "hello world",
			Attachments:  nil,
			QuickReplies: nil,
		},
	}

	legacy := map[utils.Language]*events.BroadcastTranslation{
		eng: &events.BroadcastTranslation{
			Text:         "hi @(PROPER(contact.name))",
			Attachments:  nil,
			QuickReplies: nil,
		},
	}

	template := map[utils.Language]*events.BroadcastTranslation{
		eng: &events.BroadcastTranslation{
			Text:         "hi @(title(contact.name))",
			Attachments:  nil,
			QuickReplies: nil,
		},
	}

	doctors := assets.NewGroupReference(models.DoctorsGroupUUID, "Doctors")
	doctorsOnly := []*assets.GroupReference{doctors}

	cathy := flows.NewContactReference(models.CathyUUID, "Cathy")
	cathyOnly := []*flows.ContactReference{cathy}

	// add an extra URN fo cathy
	db.MustExec(
		`INSERT INTO contacts_contacturn(org_id, contact_id, scheme, path, identity, priority) 
								  VALUES(1, $1, 'tel', '+12065551212', 'tel:+12065551212', 100)`, models.CathyID)

	// change george's URN to an invalid twitter URN so it can't be sent
	db.MustExec(
		`UPDATE contacts_contacturn SET identity = 'twitter:invalid-urn', scheme = 'twitter', path='invalid-urn' WHERE id = $1`, models.GeorgeURNID,
	)
	george := flows.NewContactReference(models.GeorgeUUID, "George")
	georgeOnly := []*flows.ContactReference{george}

	tcs := []struct {
		Translations  map[utils.Language]*events.BroadcastTranslation
		TemplateState models.TemplateState
		BaseLanguage  utils.Language
		Groups        []*assets.GroupReference
		Contacts      []*flows.ContactReference
		URNs          []urns.URN
		Queue         string
		BatchCount    int
		MsgCount      int
		MsgText       string
	}{
		{basic, models.TemplateStateEvaluated, eng, doctorsOnly, nil, nil, queue.BatchQueue, 2, 121, "hello world"},
		{basic, models.TemplateStateEvaluated, eng, doctorsOnly, georgeOnly, nil, queue.BatchQueue, 2, 121, "hello world"},
		{basic, models.TemplateStateEvaluated, eng, nil, georgeOnly, nil, queue.HandlerQueue, 1, 0, "hello world"},
		{basic, models.TemplateStateEvaluated, eng, doctorsOnly, cathyOnly, nil, queue.BatchQueue, 2, 121, "hello world"},
		{basic, models.TemplateStateEvaluated, eng, nil, cathyOnly, nil, queue.HandlerQueue, 1, 1, "hello world"},
		{basic, models.TemplateStateEvaluated, eng, nil, cathyOnly, []urns.URN{urns.URN("tel:+12065551212")}, queue.HandlerQueue, 1, 1, "hello world"},
		{basic, models.TemplateStateEvaluated, eng, nil, cathyOnly, []urns.URN{urns.URN("tel:+250700000001")}, queue.HandlerQueue, 1, 2, "hello world"},
		{basic, models.TemplateStateEvaluated, eng, nil, nil, []urns.URN{urns.URN("tel:+250700000001")}, queue.HandlerQueue, 1, 1, "hello world"},

		{legacy, models.TemplateStateLegacy, eng, nil, cathyOnly, nil, queue.HandlerQueue, 1, 1, "hi Cathy"},
		{template, models.TemplateStateUnevaluated, eng, nil, cathyOnly, nil, queue.HandlerQueue, 1, 1, "hi Cathy"},
	}

	lastNow := time.Now()
	time.Sleep(10 * time.Millisecond)

	for i, tc := range tcs {
		// handle our start task
		event := events.NewBroadcastCreatedEvent(tc.Translations, tc.BaseLanguage, tc.URNs, tc.Contacts, tc.Groups)
		bcast, err := models.NewBroadcastFromEvent(ctx, db, org, event)
		bcast.SetTemplateState(tc.TemplateState)
		assert.NoError(t, err)

		err = CreateBroadcastBatches(ctx, db, rp, bcast)
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

			err = SendBroadcastBatch(ctx, db, rp, batch)
			assert.NoError(t, err)
		}

		// assert our count of batches
		assert.Equal(t, tc.BatchCount, count, "%d: unexpected batch count", i)

		// assert our count of total msgs created
		testsuite.AssertQueryCount(t, db,
			`SELECT count(*) FROM msgs_msg WHERE org_id = 1 AND created_on > $1 AND topup_id IS NOT NULL AND text = $2`,
			[]interface{}{lastNow, tc.MsgText}, tc.MsgCount, "%d: unexpected msg count", i)

		lastNow = time.Now()
		time.Sleep(10 * time.Millisecond)
	}
}
