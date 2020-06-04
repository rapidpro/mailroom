package hooks

import (
	"encoding/json"
	"testing"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/actions"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/queue"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/stretchr/testify/assert"
)

func TestSessionTriggered(t *testing.T) {
	testsuite.Reset()
	testsuite.ResetRP()
	models.FlushCache()
	db := testsuite.DB()
	ctx := testsuite.CTX()

	org, err := models.GetOrgAssets(ctx, db, models.Org1)
	assert.NoError(t, err)

	simpleFlow, err := org.FlowByID(models.SingleMessageFlowID)
	assert.NoError(t, err)

	contactRef := &flows.ContactReference{
		UUID: models.GeorgeUUID,
	}

	groupRef := &assets.GroupReference{
		UUID: models.TestersGroupUUID,
	}

	tcs := []HookTestCase{
		HookTestCase{
			Actions: ContactActionMap{
				models.CathyID: []flows.Action{
					actions.NewStartSession(newActionUUID(), simpleFlow.FlowReference(), nil, []*flows.ContactReference{contactRef}, []*assets.GroupReference{groupRef}, nil, true),
				},
			},
			SQLAssertions: []SQLAssertion{
				{
					SQL:   "select count(*) from flows_flowrun where contact_id = $1 AND is_active = FALSE",
					Args:  []interface{}{models.CathyID},
					Count: 1,
				},
				{
					SQL:   "select count(*) from flows_flowstart where org_id = 1 AND start_type = 'F' AND flow_id = $1 AND status = 'P' AND parent_summary IS NOT NULL;",
					Args:  []interface{}{models.SingleMessageFlowID},
					Count: 1,
				},
				{
					SQL:   "select count(*) from flows_flowstart_contacts where id = 1 AND contact_id = $1",
					Args:  []interface{}{models.GeorgeID},
					Count: 1,
				},
				{
					SQL:   "select count(*) from flows_flowstart_groups where id = 1 AND contactgroup_id = $1",
					Args:  []interface{}{models.TestersGroupID},
					Count: 1,
				},
			},
			Assertions: []Assertion{
				func(t *testing.T, db *sqlx.DB, rc redis.Conn) error {
					task, err := queue.PopNextTask(rc, queue.BatchQueue)
					assert.NoError(t, err)
					assert.NotNil(t, task)
					start := models.FlowStart{}
					err = json.Unmarshal(task.Task, &start)
					assert.NoError(t, err)
					assert.Equal(t, start.CreateContact(), true)
					assert.Equal(t, []models.ContactID{models.GeorgeID}, start.ContactIDs())
					assert.Equal(t, []models.GroupID{models.TestersGroupID}, start.GroupIDs())
					assert.Equal(t, start.FlowID(), simpleFlow.ID())
					return nil
				},
			},
		},
	}

	RunHookTestCases(t, tcs)
}

func TestQuerySessionTriggered(t *testing.T) {
	testsuite.Reset()
	testsuite.ResetRP()
	models.FlushCache()
	db := testsuite.DB()
	ctx := testsuite.CTX()

	org, err := models.GetOrgAssets(ctx, db, models.Org1)
	assert.NoError(t, err)

	favoriteFlow, err := org.FlowByID(models.FavoritesFlowID)
	assert.NoError(t, err)

	sessionAction := actions.NewStartSession(newActionUUID(), favoriteFlow.FlowReference(), nil, nil, nil, nil, true)
	sessionAction.ContactQuery = "name ~ @contact.name"

	tcs := []HookTestCase{
		HookTestCase{
			Actions: ContactActionMap{
				models.CathyID: []flows.Action{sessionAction},
			},
			SQLAssertions: []SQLAssertion{
				SQLAssertion{
					SQL:   `select count(*) from flows_flowstart where flow_id = $1 AND start_type = 'F' AND status = 'P' AND query = 'name ~ "Cathy"' AND parent_summary IS NOT NULL;`,
					Args:  []interface{}{models.FavoritesFlowID},
					Count: 1,
				},
			},
			Assertions: []Assertion{
				func(t *testing.T, db *sqlx.DB, rc redis.Conn) error {
					task, err := queue.PopNextTask(rc, queue.BatchQueue)
					assert.NoError(t, err)
					assert.NotNil(t, task)
					start := models.FlowStart{}
					err = json.Unmarshal(task.Task, &start)
					assert.NoError(t, err)
					assert.Equal(t, start.CreateContact(), true)
					assert.Equal(t, 0, len(start.ContactIDs()))
					assert.Equal(t, 0, len(start.GroupIDs()))
					assert.Equal(t, `name ~ "Cathy"`, start.Query())
					assert.Equal(t, start.FlowID(), favoriteFlow.ID())
					return nil
				},
			},
		},
	}

	RunHookTestCases(t, tcs)
}
