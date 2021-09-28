package handlers_test

import (
	"testing"
	"time"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/actions"
	"github.com/nyaruka/mailroom/core/handlers"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
)

func TestMsgReceived(t *testing.T) {
	ctx, rt, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetAll)

	now := time.Now()

	tcs := []handlers.TestCase{
		{
			Actions: handlers.ContactActionMap{
				testdata.Cathy: []flows.Action{
					actions.NewSendMsg(handlers.NewActionUUID(), "Hello World", nil, nil, false),
				},
				testdata.George: []flows.Action{
					actions.NewSendMsg(handlers.NewActionUUID(), "Hello world", nil, nil, false),
				},
			},
			Msgs: handlers.ContactMsgMap{
				testdata.Cathy: testdata.InsertIncomingMsg(db, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "start", models.MsgStatusHandled),
			},
			SQLAssertions: []handlers.SQLAssertion{
				{
					SQL:   "SELECT COUNT(*) FROM contacts_contact WHERE id = $1 AND last_seen_on > $2",
					Args:  []interface{}{testdata.Cathy.ID, now},
					Count: 1,
				},
				{
					SQL:   "SELECT COUNT(*) FROM contacts_contact WHERE id = $1 AND last_seen_on IS NULL",
					Args:  []interface{}{testdata.George.ID},
					Count: 1,
				},
			},
		},
		{
			FlowType: flows.FlowTypeMessagingOffline,
			Actions: handlers.ContactActionMap{
				testdata.Bob: []flows.Action{
					actions.NewSendMsg(handlers.NewActionUUID(), "Hello World", nil, nil, false),
				},
			},
			Msgs: handlers.ContactMsgMap{
				testdata.Bob: flows.NewMsgIn(flows.MsgUUID(uuids.New()), urns.NilURN, nil, "Hi offline", nil),
			},
			SQLAssertions: []handlers.SQLAssertion{
				{
					SQL:   "SELECT COUNT(*) FROM msgs_msg WHERE contact_id = $1 AND direction = 'I'",
					Args:  []interface{}{testdata.Bob.ID},
					Count: 1,
				},
			},
		},
	}

	handlers.RunTestCases(t, ctx, rt, tcs)
}
