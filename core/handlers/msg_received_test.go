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
	testsuite.Reset()
	db := testsuite.DB()

	now := time.Now()

	tcs := []handlers.TestCase{
		{
			Actions: handlers.ContactActionMap{
				models.CathyID: []flows.Action{
					actions.NewSendMsg(handlers.NewActionUUID(), "Hello World", nil, nil, false),
				},
				models.GeorgeID: []flows.Action{
					actions.NewSendMsg(handlers.NewActionUUID(), "Hello world", nil, nil, false),
				},
			},
			Msgs: handlers.ContactMsgMap{
				models.CathyID: testdata.InsertIncomingMsg(t, db, models.Org1, models.CathyID, models.CathyURN, models.CathyURNID, "start"),
			},
			SQLAssertions: []handlers.SQLAssertion{
				{
					SQL:   "SELECT COUNT(*) FROM contacts_contact WHERE id = $1 AND last_seen_on > $2",
					Args:  []interface{}{models.CathyID, now},
					Count: 1,
				},
				{
					SQL:   "SELECT COUNT(*) FROM contacts_contact WHERE id = $1 AND last_seen_on IS NULL",
					Args:  []interface{}{models.GeorgeID},
					Count: 1,
				},
			},
		},
		{
			FlowType: flows.FlowTypeMessagingOffline,
			Actions: handlers.ContactActionMap{
				models.BobID: []flows.Action{
					actions.NewSendMsg(handlers.NewActionUUID(), "Hello World", nil, nil, false),
				},
			},
			Msgs: handlers.ContactMsgMap{
				models.BobID: flows.NewMsgIn(flows.MsgUUID(uuids.New()), urns.NilURN, nil, "Hi offline", nil),
			},
			SQLAssertions: []handlers.SQLAssertion{
				{
					SQL:   "SELECT COUNT(*) FROM msgs_msg WHERE contact_id = $1 AND direction = 'I'",
					Args:  []interface{}{models.BobID},
					Count: 1,
				},
			},
		},
	}

	handlers.RunTestCases(t, tcs)
}
