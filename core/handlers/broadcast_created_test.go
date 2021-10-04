package handlers_test

import (
	"encoding/json"
	"testing"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/actions"
	"github.com/nyaruka/mailroom/core/handlers"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/queue"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"

	"github.com/stretchr/testify/assert"
)

func TestBroadcastCreated(t *testing.T) {
	ctx, rt, _, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetAll)

	// TODO: test contacts, groups

	tcs := []handlers.TestCase{
		{
			Actions: handlers.ContactActionMap{
				testdata.Cathy: []flows.Action{
					actions.NewSendBroadcast(handlers.NewActionUUID(), "hello world", nil, nil, []urns.URN{urns.URN("tel:+12065551212")}, nil, nil, nil),
				},
			},
			SQLAssertions: []handlers.SQLAssertion{
				{
					SQL:   "select count(*) from flows_flowrun where contact_id = $1 AND is_active = FALSE",
					Args:  []interface{}{testdata.Cathy.ID},
					Count: 1,
				},
			},
			Assertions: []handlers.Assertion{
				func(t *testing.T, rt *runtime.Runtime) error {
					rc := rt.RP.Get()
					defer rc.Close()

					task, err := queue.PopNextTask(rc, queue.HandlerQueue)
					assert.NoError(t, err)
					assert.NotNil(t, task)
					bcast := models.Broadcast{}
					err = json.Unmarshal(task.Task, &bcast)
					assert.NoError(t, err)
					assert.Nil(t, bcast.ContactIDs())
					assert.Nil(t, bcast.GroupIDs())
					assert.Equal(t, 1, len(bcast.URNs()))
					return nil
				},
			},
		},
	}

	handlers.RunTestCases(t, ctx, rt, tcs)
}
