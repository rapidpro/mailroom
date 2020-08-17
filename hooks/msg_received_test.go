package hooks

import (
	"testing"
	"time"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/actions"
	"github.com/nyaruka/goflow/utils/uuids"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/testsuite"
)

func TestMsgReceived(t *testing.T) {
	testsuite.Reset()
	db := testsuite.DB()

	now := time.Now()

	tcs := []HookTestCase{
		{
			Actions: ContactActionMap{
				models.CathyID: []flows.Action{
					actions.NewSendMsg(newActionUUID(), "Hello World", nil, nil, false),
				},
				models.GeorgeID: []flows.Action{
					actions.NewSendMsg(newActionUUID(), "Hello world", nil, nil, false),
				},
			},
			Msgs: ContactMsgMap{
				models.CathyID: createIncomingMsg(db, models.Org1, models.CathyID, models.CathyURN, models.CathyURNID, "start"),
			},
			SQLAssertions: []SQLAssertion{
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
			Actions: ContactActionMap{
				models.BobID: []flows.Action{
					actions.NewSendMsg(newActionUUID(), "Hello World", nil, nil, false),
				},
			},
			Msgs: ContactMsgMap{
				models.BobID: flows.NewMsgIn(flows.MsgUUID(uuids.New()), urns.NilURN, nil, "Hi offline", nil),
			},
			SQLAssertions: []SQLAssertion{
				{
					SQL:   "SELECT COUNT(*) FROM msgs_msg WHERE contact_id = $1 AND direction = 'I'",
					Args:  []interface{}{models.BobID},
					Count: 1,
				},
			},
		},
	}

	RunHookTestCases(t, tcs)
}
