package handlers_test

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/nyaruka/gocommon/aws/dynamo"
	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/actions"
	"github.com/nyaruka/goflow/flows/definition"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/goflow/flows/routers"
	"github.com/nyaruka/goflow/flows/triggers"
	"github.com/nyaruka/goflow/test"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/runner"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type ContactEventMap map[flows.ContactUUID][]flows.Event

func (m *ContactEventMap) UnmarshalJSON(d []byte) error {
	*m = make(ContactEventMap)

	var raw map[flows.ContactUUID][]json.RawMessage
	if err := json.Unmarshal(d, &raw); err != nil {
		return err
	}

	for contactUUID, v := range raw {
		unmarshaled := make([]flows.Event, len(v))
		for i := range v {
			var err error
			unmarshaled[i], err = events.Read(v[i])
			if err != nil {
				return err
			}
		}
		(*m)[contactUUID] = unmarshaled
	}
	return nil
}

type ContactActionMap map[flows.ContactUUID][]flows.Action

func (m *ContactActionMap) UnmarshalJSON(d []byte) error {
	*m = make(ContactActionMap)

	var raw map[flows.ContactUUID][]json.RawMessage
	if err := json.Unmarshal(d, &raw); err != nil {
		return err
	}

	for contactUUID, v := range raw {
		unmarshaled := make([]flows.Action, len(v))
		for i := range v {
			var err error
			unmarshaled[i], err = actions.Read(v[i])
			if err != nil {
				return err
			}
		}
		(*m)[contactUUID] = unmarshaled
	}
	return nil
}

type TestCase struct {
	Label           string                          `json:"label"`
	BroadcastID     models.BroadcastID              `json:"broadcast_id,omitempty"`
	Events          ContactEventMap                 `json:"events"`
	Actions         ContactActionMap                `json:"actions,omitempty"`
	UserID          models.UserID                   `json:"user_id,omitempty"`
	DBAssertions    []*assertdb.Assert              `json:"db_assertions,omitempty"`
	ExpectedTasks   map[string][]testsuite.TaskInfo `json:"expected_tasks,omitempty"`
	ExpectedHistory []*dynamo.Item                  `json:"expected_history,omitempty"`
	IndexedMessages []testsuite.IndexedMessage       `json:"indexed_messages,omitempty"`
}

func runTests(t *testing.T, rt *runtime.Runtime, truthFile string) {
	ctx := t.Context()
	tcs := make([]TestCase, 0, 20)
	tcJSON := testsuite.ReadFile(t, truthFile)

	jsonx.MustUnmarshal(tcJSON, &tcs)

	models.FlushCache()

	oa, err := models.GetOrgAssets(ctx, rt, testdb.Org1.ID)
	assert.NoError(t, err)

	test.MockUniverse()

	// clear any stale opensearch data from previous test runs
	testsuite.GetIndexedMessages(t, rt, true)

	for i, tc := range tcs {
		scenes := make([]*runner.Scene, 4)
		msgEvents := make([]*events.MsgReceived, 4)

		for i, c := range []*testdb.Contact{testdb.Ann, testdb.Bob, testdb.Cat, testdb.Dan} {
			mc, contact, _ := c.Load(t, rt, oa)
			scenes[i] = runner.NewScene(mc, contact)

			if tc.BroadcastID != models.NilBroadcastID {
				bcast, err := models.GetBroadcastByID(ctx, rt.DB, tc.BroadcastID)
				require.NoError(t, err)

				scenes[i].Broadcast = bcast
			}

			for _, e := range tc.Events[c.UUID] {
				if me, ok := e.(*events.MsgReceived); ok {
					scenes[i].IncomingMsg = insertTestMessage(t, rt, oa, c, me.Msg)
					contact.SetLastSeenOn(me.CreatedOn())
					msgEvents[i] = me
				}

				err := scenes[i].AddEvent(ctx, rt, oa, e, tc.UserID, "")
				require.NoError(t, err)
			}
		}

		if tc.Actions != nil {
			// reuse id from one of our real flows
			flowUUID := testdb.Favorites.UUID

			// create dynamic flow definition to test actions
			testFlow := createTestFlow(t, flowUUID, tc.Actions)
			flowDef, err := json.Marshal(testFlow)
			require.NoError(t, err)

			rt.DB.MustExec(`UPDATE flows_flow SET name = 'Test Flow' WHERE id = $1`, testdb.Favorites.ID)
			rt.DB.MustExec(`UPDATE flows_flowrevision SET definition = $2 WHERE flow_id = $1`, testdb.Favorites.ID, string(flowDef))

			oa2, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdb.Org1.ID, models.RefreshFlows)
			assert.NoError(t, err)

			for i, scene := range scenes {
				if tc.Actions[scene.ContactUUID()] != nil {
					msgEvent := msgEvents[i]
					var trig flows.Trigger

					if msgEvent != nil {
						trig = triggers.NewBuilder(testFlow.Reference(false)).MsgReceived(msgEvent).Build()
					} else {
						trig = triggers.NewBuilder(testFlow.Reference(false)).Manual().Build()
					}

					err = scene.StartSession(ctx, rt, oa2, trig, true)
					require.NoError(t, err)
				}
			}
		}

		err = runner.BulkCommit(ctx, rt, oa, scenes)
		require.NoError(t, err)

		// clone test case and populate with actual values
		actual := tc
		actual.ExpectedTasks = testsuite.GetQueuedTasks(t, rt)
		actual.ExpectedHistory = testsuite.GetHistoryItems(t, rt, true, test.MockStartTime)
		actual.IndexedMessages = testsuite.GetIndexedMessages(t, rt, true)

		actual.DBAssertions = make([]*assertdb.Assert, len(tc.DBAssertions))
		for i, dba := range tc.DBAssertions {
			actual.DBAssertions[i] = dba.Actual(t, rt.DB)
		}

		testsuite.ClearTasks(t, rt)

		if !test.UpdateSnapshots {
			// now check our assertions
			for _, dba := range tc.DBAssertions {
				dba.Check(t, rt.DB, "%s: assertion for query '%s' failed", tc.Label, dba.Query)
			}

			if tc.ExpectedTasks == nil {
				tc.ExpectedTasks = map[string][]testsuite.TaskInfo{}
			}
			test.AssertEqualJSON(t, jsonx.MustMarshal(tc.ExpectedTasks), jsonx.MustMarshal(actual.ExpectedTasks), "%s: unexpected tasks", tc.Label)

			if tc.ExpectedHistory == nil {
				tc.ExpectedHistory = []*dynamo.Item{}
			}
			test.AssertEqualJSON(t, jsonx.MustMarshal(tc.ExpectedHistory), jsonx.MustMarshal(actual.ExpectedHistory), "%s: event history mismatch", tc.Label)

			if tc.IndexedMessages == nil {
				tc.IndexedMessages = []testsuite.IndexedMessage{}
			}
			if actual.IndexedMessages == nil {
				actual.IndexedMessages = []testsuite.IndexedMessage{}
			}
			test.AssertEqualJSON(t, jsonx.MustMarshal(tc.IndexedMessages), jsonx.MustMarshal(actual.IndexedMessages), "%s: indexed messages mismatch", tc.Label)
		} else {
			tcs[i] = actual
		}
	}

	// update if we are meant to
	if test.UpdateSnapshots {
		truth, err := jsonx.MarshalPretty(tcs)
		require.NoError(t, err)

		err = os.WriteFile(truthFile, truth, 0644)
		require.NoError(t, err, "failed to update truth file")
	}
}

func insertTestMessage(t *testing.T, rt *runtime.Runtime, oa *models.OrgAssets, c *testdb.Contact, msg *flows.MsgIn) *models.MsgInRef {
	ch := oa.ChannelByUUID(msg.Channel().UUID)
	tch := &testdb.Channel{ID: ch.ID(), UUID: ch.UUID(), Type: ch.Type()}

	m := testdb.InsertIncomingMsg(t, rt, testdb.Org1, flows.NewEventUUID(), tch, c, msg.Text(), models.MsgStatusPending)
	return &models.MsgInRef{UUID: m.UUID}
}

// createTestFlow creates a flow that starts with a split by contact id
// and then routes the contact to a node where all the actions in the
// test case are present.
//
// It returns the completed flow.
func createTestFlow(t *testing.T, uuid assets.FlowUUID, actions ContactActionMap) flows.Flow {
	categoryUUIDs := make([]flows.CategoryUUID, len(actions))
	exitUUIDs := make([]flows.ExitUUID, len(actions))
	i := 0
	for range actions {
		categoryUUIDs[i] = flows.CategoryUUID(uuids.NewV4())
		exitUUIDs[i] = flows.ExitUUID(uuids.NewV4())
		i++
	}
	defaultCategoryUUID := flows.CategoryUUID(uuids.NewV4())
	defaultExitUUID := flows.ExitUUID(uuids.NewV4())

	cases := make([]*routers.Case, len(actions))
	categories := make([]flows.Category, len(actions))
	exits := make([]flows.Exit, len(actions))
	exitNodes := make([]flows.Node, len(actions))
	i = 0
	for contactUUID, actions := range actions {
		cases[i] = routers.NewCase(uuids.NewV4(), "has_any_word", []string{string(contactUUID)}, categoryUUIDs[i])

		exitNodes[i] = definition.NewNode(
			flows.NewNodeUUID(),
			actions,
			nil,
			[]flows.Exit{definition.NewExit(flows.ExitUUID(uuids.NewV4()), "")},
		)

		categories[i] = routers.NewCategory(categoryUUIDs[i], fmt.Sprintf("Contact %s", contactUUID), exitUUIDs[i])

		exits[i] = definition.NewExit(exitUUIDs[i], exitNodes[i].UUID())
		i++
	}

	// create our router
	categories = append(categories, routers.NewCategory(defaultCategoryUUID, "Other", defaultExitUUID))
	exits = append(exits, definition.NewExit(defaultExitUUID, flows.NodeUUID("")))
	router := routers.NewSwitch(nil, "", categories, "@contact.uuid", cases, defaultCategoryUUID)

	// and our entry node
	entry := definition.NewNode(flows.NewNodeUUID(), nil, router, exits)

	nodes := []flows.Node{entry}
	nodes = append(nodes, exitNodes...)

	// we have our nodes, lets create our flow
	flow, err := definition.NewFlow(
		uuid,
		"Test Flow",
		"eng",
		flows.FlowTypeMessaging,
		1,
		300,
		definition.NewLocalization(),
		nodes,
		nil,
		nil,
	)
	require.NoError(t, err)

	return flow
}
