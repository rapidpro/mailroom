package hooks

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/gocommon/urns"
	"github.com/greatnonprofits-nfp/goflow/assets"
	"github.com/greatnonprofits-nfp/goflow/flows"
	"github.com/greatnonprofits-nfp/goflow/flows/definition"
	"github.com/greatnonprofits-nfp/goflow/flows/routers"
	"github.com/greatnonprofits-nfp/goflow/flows/triggers"
	"github.com/greatnonprofits-nfp/goflow/utils"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/runner"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type ContactActionMap map[models.ContactID][]flows.Action
type ContactMsgMap map[models.ContactID]*flows.MsgIn

type HookTestCase struct {
	Actions       ContactActionMap
	Msgs          ContactMsgMap
	Assertions    []Assertion
	SQLAssertions []SQLAssertion
}

type Assertion func(t *testing.T, db *sqlx.DB, rc redis.Conn) error

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
	categoryUUIDs := make([]flows.CategoryUUID, len(tc.Actions))
	exitUUIDs := make([]flows.ExitUUID, len(tc.Actions))
	i := 0
	for range tc.Actions {
		categoryUUIDs[i] = flows.CategoryUUID(utils.NewUUID())
		exitUUIDs[i] = flows.ExitUUID(utils.NewUUID())
		i++
	}
	defaultCategoryUUID := flows.CategoryUUID(utils.NewUUID())
	defaultExitUUID := flows.ExitUUID(utils.NewUUID())

	cases := make([]*routers.Case, len(tc.Actions))
	categories := make([]*routers.Category, len(tc.Actions))
	exits := make([]flows.Exit, len(tc.Actions))
	exitNodes := make([]flows.Node, len(tc.Actions))
	i = 0
	for cid, actions := range tc.Actions {
		cases[i] = routers.NewCase(
			utils.NewUUID(),
			"has_any_word",
			[]string{fmt.Sprintf("%d", cid)},
			categoryUUIDs[i],
		)

		exitNodes[i] = definition.NewNode(
			flows.NodeUUID(utils.NewUUID()),
			actions,
			nil,
			[]flows.Exit{definition.NewExit(flows.ExitUUID(utils.NewUUID()), "")},
		)

		categories[i] = routers.NewCategory(
			categoryUUIDs[i],
			fmt.Sprintf("Contact %d", cid),
			exitUUIDs[i],
		)

		exits[i] = definition.NewExit(
			exitUUIDs[i],
			exitNodes[i].UUID(),
		)
		i++
	}

	// create our router
	categories = append(categories, routers.NewCategory(
		defaultCategoryUUID,
		"Other",
		defaultExitUUID,
	))
	exits = append(exits, definition.NewExit(
		defaultExitUUID,
		flows.NodeUUID(""),
	))

	router := routers.NewSwitchRouter(nil, "", categories, "@contact.id", cases, defaultCategoryUUID)

	// and our entry node
	entry := definition.NewNode(
		flows.NodeUUID(utils.NewUUID()),
		nil,
		router,
		exits,
	)

	nodes := []flows.Node{entry}
	nodes = append(nodes, exitNodes...)

	// we have our nodes, lets create our flow
	flow, err := definition.NewFlow(
		uuid,
		"Test Flow",
		utils.Language("eng"),
		flows.FlowTypeMessaging,
		1,
		300,
		definition.NewLocalization(),
		nodes,
		nil,
	)
	require.NoError(t, err)

	return flow
}

func createIncomingMsg(db *sqlx.DB, orgID models.OrgID, contactID models.ContactID, urn urns.URN, urnID models.URNID, text string) *flows.MsgIn {
	msgUUID := flows.MsgUUID(utils.NewUUID())
	var msgID flows.MsgID

	err := db.Get(&msgID,
		`INSERT INTO msgs_msg(uuid, text, created_on, direction, status, visibility, msg_count, error_count, next_attempt, contact_id, contact_urn_id, org_id)
	  						  VALUES($1, $2, NOW(), 'I', 'P', 'V', 1, 0, NOW(), $3, $4, $5) RETURNING id`,
		msgUUID, text, contactID, urnID, orgID)
	if err != nil {
		panic(err)
	}

	msg := flows.NewMsgIn(msgUUID, urn, nil, text, nil)
	msg.SetID(msgID)
	return msg
}

func RunActionTestCases(t *testing.T, tcs []HookTestCase) {
	models.FlushCache()

	db := testsuite.DB()
	ctx := testsuite.CTX()
	rp := testsuite.RP()

	org, err := models.GetOrgAssets(ctx, db, models.OrgID(1))
	assert.NoError(t, err)

	// reuse id from one of our real flows
	flowID := models.FavoritesFlowID
	flowUUID := assets.FlowUUID(utils.NewUUID())

	for i, tc := range tcs {
		// build our flow for this test case
		flowDef := CreateTestFlow(t, flowUUID, tc)

		// add it to our org
		flow, err := org.SetFlow(flowID, flowDef)
		assert.NoError(t, err)

		options := runner.NewStartOptions()
		options.CommitHook = func(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, sessions []*models.Session) error {
			for _, s := range sessions {
				msg := tc.Msgs[s.ContactID()]
				if msg != nil {
					s.SetIncomingMsg(msg.ID(), "")
				}
			}
			return nil
		}
		options.TriggerBuilder = func(contact *flows.Contact) flows.Trigger {
			msg := tc.Msgs[models.ContactID(contact.ID())]
			if msg == nil {
				return triggers.NewManualTrigger(org.Env(), flow.FlowReference(), contact, nil)
			}
			return triggers.NewMsgTrigger(org.Env(), flow.FlowReference(), contact, msg, nil)
		}

		_, err = runner.StartFlow(ctx, db, rp, org, flow, []models.ContactID{models.CathyID, models.BobID, models.GeorgeID, models.AlexandriaID}, options)
		assert.NoError(t, err)

		// now check our assertions
		time.Sleep(1 * time.Second)
		for ii, a := range tc.SQLAssertions {
			testsuite.AssertQueryCount(t, db, a.SQL, a.Args, a.Count, "%d:%d: mismatch in expected count for query: %s", i, ii, a.SQL)
		}

		rc := rp.Get()
		for ii, a := range tc.Assertions {
			err := a(t, db, rc)
			assert.NoError(t, err, "%d: %d error checking assertion", i, ii)
		}
		rc.Close()
	}
}
