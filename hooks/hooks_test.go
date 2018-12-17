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
	ageUUID    models.FieldUUID
	genderUUID models.FieldUUID
)

type ContactActionMap map[flows.ContactID][]flows.Action
type ContactMsgMap map[flows.ContactID]*flows.MsgIn

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

	// populate age and gender UUIDs
	db := testsuite.DB()
	ctx := testsuite.CTX()

	org, err := models.GetOrgAssets(ctx, db, models.Org1)
	if err != nil {
		panic(err)
	}

	f := org.FieldByKey("age")
	ageUUID = f.UUID()

	f = org.FieldByKey("gender")
	genderUUID = f.UUID()

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
	cases := make([]*routers.Case, len(tc.Actions))
	i = 0
	for cid, actions := range tc.Actions {
		cases[i] = routers.NewCase(
			utils.NewUUID(),
			"has_any_word",
			[]string{fmt.Sprintf("%d", cid)},
			false,
			exitUUIDs[i],
		)

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
		nil,
		router,
		exits,
	)

	nodes := []flows.Node{entry}
	nodes = append(nodes, exitNodes...)

	// we have our nodes, lets create our flow
	flow := definition.NewFlow(
		uuid,
		"Test Flow",
		"12.0",
		utils.Language("eng"),
		flows.FlowTypeMessaging,
		1,
		300,
		nil,
		nodes,
		nil,
	)

	return flow
}

func createIncomingMsg(db *sqlx.DB, orgID models.OrgID, contactID flows.ContactID, urn urns.URN, urnID models.URNID, text string) *flows.MsgIn {
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
	flowID := models.FlowID(1)
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
				msg := tc.Msgs[s.Contact().ID()]
				if msg != nil {
					s.SetIncomingMsg(msg.ID(), "")
				}
			}
			return nil
		}
		options.TriggerBuilder = func(contact *flows.Contact) flows.Trigger {
			msg := tc.Msgs[contact.ID()]
			if msg == nil {
				return triggers.NewManualTrigger(org.Env(), flow.FlowReference(), contact, nil, time.Now())
			} else {
				return triggers.NewMsgTrigger(org.Env(), flow.FlowReference(), contact, msg, nil, time.Now())
			}
		}

		_, err = runner.StartFlow(ctx, db, rp, org, flow, []flows.ContactID{models.Cathy, models.Bob, models.Evan}, options)
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
