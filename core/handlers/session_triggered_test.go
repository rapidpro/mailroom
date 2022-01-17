package handlers_test

import (
	"encoding/json"
	"testing"

	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/assets"
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

func TestSessionTriggered(t *testing.T) {
	ctx, rt, _, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetAll)

	oa, err := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
	assert.NoError(t, err)

	simpleFlow, err := oa.FlowByID(testdata.SingleMessage.ID)
	assert.NoError(t, err)

	contactRef := &flows.ContactReference{
		UUID: testdata.George.UUID,
	}

	groupRef := &assets.GroupReference{
		UUID: testdata.TestersGroup.UUID,
	}

	uuids.SetGenerator(uuids.NewSeededGenerator(1234567))
	defer uuids.SetGenerator(uuids.DefaultGenerator)

	tcs := []handlers.TestCase{
		{
			Actions: handlers.ContactActionMap{
				testdata.Cathy: []flows.Action{
					actions.NewStartSession(handlers.NewActionUUID(), simpleFlow.FlowReference(), nil, []*flows.ContactReference{contactRef}, []*assets.GroupReference{groupRef}, nil, true),
				},
			},
			SQLAssertions: []handlers.SQLAssertion{
				{
					SQL:   "select count(*) from flows_flowrun where contact_id = $1 AND is_active = FALSE",
					Args:  []interface{}{testdata.Cathy.ID},
					Count: 1,
				},
				{
					SQL:   "select count(*) from flows_flowstart where org_id = 1 AND start_type = 'F' AND flow_id = $1 AND status = 'P' AND parent_summary IS NOT NULL AND session_history IS NOT NULL;",
					Args:  []interface{}{testdata.SingleMessage.ID},
					Count: 1,
				},
				{
					SQL:   "select count(*) from flows_flowstart_contacts where id = 1 AND contact_id = $1",
					Args:  []interface{}{testdata.George.ID},
					Count: 1,
				},
				{
					SQL:   "select count(*) from flows_flowstart_groups where id = 1 AND contactgroup_id = $1",
					Args:  []interface{}{testdata.TestersGroup.ID},
					Count: 1,
				},
			},
			Assertions: []handlers.Assertion{
				func(t *testing.T, rt *runtime.Runtime) error {
					rc := rt.RP.Get()
					defer rc.Close()

					task, err := queue.PopNextTask(rc, queue.BatchQueue)
					assert.NoError(t, err)
					assert.NotNil(t, task)
					start := models.FlowStart{}
					err = json.Unmarshal(task.Task, &start)
					assert.NoError(t, err)
					assert.True(t, start.CreateContact())
					assert.Equal(t, []models.ContactID{testdata.George.ID}, start.ContactIDs())
					assert.Equal(t, []models.GroupID{testdata.TestersGroup.ID}, start.GroupIDs())
					assert.Equal(t, simpleFlow.ID(), start.FlowID())
					assert.JSONEq(t, `{"parent_uuid":"39a9f95e-3641-4d19-95e0-ed866f27c829", "ancestors":1, "ancestors_since_input":1}`, string(start.SessionHistory()))
					return nil
				},
			},
		},
	}

	handlers.RunTestCases(t, ctx, rt, tcs)
}

func TestQuerySessionTriggered(t *testing.T) {
	ctx, rt, _, rp := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetAll)

	oa, err := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
	assert.NoError(t, err)

	favoriteFlow, err := oa.FlowByID(testdata.Favorites.ID)
	assert.NoError(t, err)

	sessionAction := actions.NewStartSession(handlers.NewActionUUID(), favoriteFlow.FlowReference(), nil, nil, nil, nil, true)
	sessionAction.ContactQuery = "name ~ @contact.name"

	tcs := []handlers.TestCase{
		{
			Actions: handlers.ContactActionMap{
				testdata.Cathy: []flows.Action{sessionAction},
			},
			SQLAssertions: []handlers.SQLAssertion{
				{
					SQL:   `select count(*) from flows_flowstart where flow_id = $1 AND start_type = 'F' AND status = 'P' AND query = 'name ~ "Cathy"' AND parent_summary IS NOT NULL;`,
					Args:  []interface{}{testdata.Favorites.ID},
					Count: 1,
				},
			},
			Assertions: []handlers.Assertion{
				func(t *testing.T, rt *runtime.Runtime) error {
					rc := rp.Get()
					defer rc.Close()

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

	handlers.RunTestCases(t, ctx, rt, tcs)
}
