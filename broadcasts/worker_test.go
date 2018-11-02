package broadcasts

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/nyaruka/goflow/assets"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom"
	_ "github.com/nyaruka/mailroom/hooks"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/queue"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	testsuite.Reset()
	os.Exit(m.Run())
}

func TestBroadcasts(t *testing.T) {
	testsuite.Reset()
	ctx := testsuite.CTX()
	rp := testsuite.RP()
	db := testsuite.DB()
	rc := testsuite.RC()
	defer rc.Close()

	org, err := models.GetOrgAssets(ctx, db, models.OrgID(1))
	assert.NoError(t, err)

	eng := utils.Language("eng")
	basic := map[utils.Language]*events.BroadcastTranslation{
		eng: &events.BroadcastTranslation{
			Text:         "hello world",
			Attachments:  nil,
			QuickReplies: nil,
		},
	}

	doctors := assets.NewGroupReference(assets.GroupUUID("85a5a793-4741-4896-b55e-05af65f3c0fa"), "Doctors")
	doctorsOnly := []*assets.GroupReference{doctors}

	cathy := flows.NewContactReference(flows.ContactUUID("9a8dca3f-dcf4-46bc-a3dc-a707b95b6664"), "Cathy Quincy")
	cathyOnly := []*flows.ContactReference{cathy}

	// add an extra URN fo cathy
	db.MustExec(
		`INSERT INTO contacts_contacturn(org_id, contact_id, scheme, path, identity, priority) 
								  VALUES(1, $1, 'tel', '+12065551212', 'tel:+12065551212', 100)`, 42)

	// george has an invalid twitter URN, so he won't cause messages to be sent
	george := flows.NewContactReference(flows.ContactUUID("d03200d4-fc29-41b9-9894-61f198c26148"), "George Poots")
	georgeOnly := []*flows.ContactReference{george}

	tcs := []struct {
		Translations map[utils.Language]*events.BroadcastTranslation
		BaseLanguage utils.Language
		Groups       []*assets.GroupReference
		Contacts     []*flows.ContactReference
		URNs         []urns.URN
		Queue        string
		BatchCount   int
		MsgCount     int
	}{
		{basic, eng, doctorsOnly, nil, nil, mailroom.BatchQueue, 21, 1797},
		{basic, eng, doctorsOnly, georgeOnly, nil, mailroom.BatchQueue, 21, 1797},
		{basic, eng, nil, georgeOnly, nil, mailroom.HandlerQueue, 1, 0},
		{basic, eng, doctorsOnly, cathyOnly, nil, mailroom.BatchQueue, 21, 1797},
		{basic, eng, nil, cathyOnly, nil, mailroom.HandlerQueue, 1, 1},
		{basic, eng, nil, cathyOnly, []urns.URN{urns.URN("tel:+12065551212")}, mailroom.HandlerQueue, 1, 1},
		{basic, eng, nil, cathyOnly, []urns.URN{urns.URN("tel:+250700000001")}, mailroom.HandlerQueue, 1, 2},
		{basic, eng, nil, nil, []urns.URN{urns.URN("tel:+250700000001")}, mailroom.HandlerQueue, 1, 1},
	}

	lastNow := time.Now()
	time.Sleep(10 * time.Millisecond)

	for i, tc := range tcs {
		// handle our start task
		event := events.NewBroadcastCreatedEvent(tc.Translations, tc.BaseLanguage, tc.URNs, tc.Contacts, tc.Groups)
		bcast, err := models.NewBroadcastFromEvent(ctx, db, org, event)
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
			assert.Equal(t, mailroom.SendBroadcastBatchType, task.Type)
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
			`SELECT count(*) FROM msgs_msg WHERE org_id = 1 AND created_on > $1`,
			[]interface{}{lastNow}, tc.MsgCount, "%d: unexpected msg count", i)

		lastNow = time.Now()
		time.Sleep(10 * time.Millisecond)
	}
}
