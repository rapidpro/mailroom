package hooks_test

import (
	"encoding/json"
	"testing"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/actions"
	"github.com/nyaruka/mailroom/hooks"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/queue"
	"github.com/nyaruka/mailroom/testsuite"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
)

func TestBroadcastCreated(t *testing.T) {
	testsuite.Reset()

	// TODO: test contacts, groups

	tcs := []hooks.TestCase{
		{
			Actions: hooks.ContactActionMap{
				models.CathyID: []flows.Action{
					actions.NewSendBroadcast(hooks.NewActionUUID(), "hello world", nil, nil, []urns.URN{urns.URN("tel:+12065551212")}, nil, nil, nil),
				},
			},
			SQLAssertions: []hooks.SQLAssertion{
				{
					SQL:   "select count(*) from flows_flowrun where contact_id = $1 AND is_active = FALSE",
					Args:  []interface{}{models.CathyID},
					Count: 1,
				},
			},
			Assertions: []hooks.Assertion{
				func(t *testing.T, db *sqlx.DB, rc redis.Conn) error {
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

	hooks.RunTestCases(t, tcs)
}
