package hooks

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/definition"
	"github.com/nyaruka/goflow/flows/routers"
	"github.com/nyaruka/goflow/flows/triggers"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/runner"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/stretchr/testify/assert"
)

var (
	httpClient = utils.NewHTTPClient("mailroom")
)

type ContactEventMap map[flows.ContactID][]flows.Event
type ContactActionMap map[flows.ContactID][]flows.Action

const (
	Cathy = flows.ContactID(43)
	Bob   = flows.ContactID(58)
	Evan  = flows.ContactID(47)
)

type HookTestCase struct {
	Actions    ContactActionMap
	Assertions []SQLAssertion
}

type SQLAssertion struct {
	SQL   string
	Args  []interface{}
	Count int
}

func newActionUUID() flows.ActionUUID {
	return flows.ActionUUID(utils.NewUUID())
}

func TestMain(m *testing.M) {
	testsuite.Reset()
	os.Exit(m.Run())
}

// CreateTestFlow creates a flow that starts with a spit by contact id
// and then routes the contact to a node where all the actions in the
// test case are present.
//
// It returns the completed flow.
func CreateTestFlow(t *testing.T, uuid assets.FlowUUID, tc HookTestCase) flows.Flow {
	exitUUIDs := make([]flows.ExitUUID, len(tc.Actions))
	i := 0
	for _ = range tc.Actions {
		exitUUIDs[i] = flows.ExitUUID(utils.NewUUID())
		i++
	}
	defaultExitUUID := flows.ExitUUID(utils.NewUUID())

	exits := make([]flows.Exit, len(tc.Actions))
	exitNodes := make([]flows.Node, len(tc.Actions))
	cases := make([]routers.Case, len(tc.Actions))
	i = 0
	for cid, actions := range tc.Actions {
		cases[i] = routers.Case{
			UUID:        utils.NewUUID(),
			Type:        "has_any_word",
			Arguments:   []string{fmt.Sprintf("%d", cid)},
			OmitOperand: false,
			ExitUUID:    exitUUIDs[i],
		}

		exitNodes[i] = definition.NewNode(
			flows.NodeUUID(utils.NewUUID()),
			actions,
			nil,
			nil,
			nil,
		)

		exits[i] = definition.NewExit(
			exitUUIDs[i],
			exitNodes[i].UUID(),
			fmt.Sprintf("Contact %d", cid),
		)
		i++
	}

	// create our router
	router := routers.NewSwitchRouter(defaultExitUUID, "@contact.id", cases, "")
	exits = append(exits, definition.NewExit(
		defaultExitUUID,
		flows.NodeUUID(""),
		"Other",
	))

	// and our entry node
	entry := definition.NewNode(
		flows.NodeUUID(utils.NewUUID()),
		nil,
		router,
		exits,
		nil,
	)

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
		nil,
		nodes,
		nil,
	)

	if err != nil {
		t.Errorf("error creating test flow: %s", err)
	}

	return flow
}

func RunActionTestCases(t *testing.T, tcs []HookTestCase) {
	models.FlushCache()

	db := testsuite.DB()
	ctx := testsuite.CTX()
	rp := testsuite.RP()

	org, err := models.GetOrgAssets(ctx, db, models.OrgID(1))
	assert.NoError(t, err)

	// reuse id from one of our real flows
	flowID := models.FlowID(1)
	flowUUID := assets.FlowUUID(utils.NewUUID())

	for i, tc := range tcs {
		// build our flow for this test case
		flowDef := CreateTestFlow(t, flowUUID, tc)

		// add it to our org
		flow, err := org.SetFlow(flowID, flowDef)
		assert.NoError(t, err)

		options := runner.NewStartOptions()
		options.TriggerBuilder = func(contact *flows.Contact) flows.Trigger {
			return triggers.NewManualTrigger(org.Env(), contact, flow.FlowReference(), nil, time.Now())
		}

		_, err = runner.StartFlowForContacts(ctx, db, rp, org, flow, []flows.ContactID{Cathy, Bob, Evan}, options)
		assert.NoError(t, err)

		// now check our assertions
		time.Sleep(1 * time.Second)
		for ii, a := range tc.Assertions {
			testsuite.AssertQueryCount(t, db, a.SQL, a.Args, a.Count, "%d:%d: mismatch in expected count for query: %s", i, ii, a.SQL)
		}
	}
}
