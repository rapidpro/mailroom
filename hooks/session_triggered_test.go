package hooks

import (
	"encoding/json"
	"testing"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/actions"
	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/queue"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/stretchr/testify/assert"
)

func TestSessionTriggered(t *testing.T) {
	testsuite.Reset()
	testsuite.ResetRP()
	db := testsuite.DB()
	ctx := testsuite.CTX()

	org, err := models.GetOrgAssets(ctx, db, Org1)
	assert.NoError(t, err)

	flow, err := org.FlowByID(1)
	assert.NoError(t, err)

	// TODO: test contacts, urns, groups

	tcs := []HookTestCase{
		HookTestCase{
			Actions: ContactActionMap{
				Cathy: []flows.Action{
					actions.NewStartSessionAction(newActionUUID(), flow.FlowReference(), nil, nil, nil, nil, true),
				},
			},
			SQLAssertions: []SQLAssertion{
				SQLAssertion{
					SQL:   "select count(*) from flows_flowrun where contact_id = $1 and is_active = FALSE",
					Args:  []interface{}{Cathy},
					Count: 1,
				},
			},
			Assertions: []Assertion{
				func(t *testing.T, db *sqlx.DB, rc redis.Conn) error {
					task, err := queue.PopNextTask(rc, mailroom.HandlerQueue)
					assert.NoError(t, err)
					assert.NotNil(t, task)
					start := models.FlowStart{}
					err = json.Unmarshal(task.Task, &start)
					assert.NoError(t, err)
					assert.Equal(t, start.CreateContact(), true)
					assert.Nil(t, start.ContactIDs())
					assert.Nil(t, start.GroupIDs())
					assert.Equal(t, start.FlowID(), models.FlowID(1))
					return nil
				},
			},
		},
	}

	RunActionTestCases(t, tcs)
}
