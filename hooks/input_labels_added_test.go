package hooks

import (
	"testing"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/mailroom/testsuite"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/actions"
)

func TestInputLabelsAdded(t *testing.T) {
	db := testsuite.DB()

	reporting := assets.NewLabelReference(assets.LabelUUID("981a4378-4c25-4ba4-bf46-74f357e7f058"), "Reporting")
	testing := assets.NewLabelReference(assets.LabelUUID("57ec7d9b-f00d-4bda-8db0-0037b5f50c8d"), "Testing")

	msg1 := createIncomingMsg(db, Org1, Cathy, CathyURN, CathyURNID, "start")
	msg2 := createIncomingMsg(db, Org1, Bob, BobURN, BobURNID, "start")

	tcs := []HookTestCase{
		HookTestCase{
			Actions: ContactActionMap{
				Cathy: []flows.Action{
					actions.NewAddInputLabelsAction(newActionUUID(), []*assets.LabelReference{reporting}),
					actions.NewAddInputLabelsAction(newActionUUID(), []*assets.LabelReference{testing}),
					actions.NewAddInputLabelsAction(newActionUUID(), []*assets.LabelReference{reporting}),
				},
				Bob: []flows.Action{},
				Evan: []flows.Action{
					actions.NewAddInputLabelsAction(newActionUUID(), []*assets.LabelReference{testing}),
					actions.NewAddInputLabelsAction(newActionUUID(), []*assets.LabelReference{reporting}),
				},
			},
			Msgs: ContactMsgMap{
				Cathy: msg1,
				Bob:   msg2,
			},
			Assertions: []SQLAssertion{
				SQLAssertion{
					SQL:   "select count(*) from msgs_msg_labels WHERE msg_id = $1",
					Args:  []interface{}{msg1.ID()},
					Count: 2,
				},
				SQLAssertion{
					SQL:   "select count(*) from msgs_msg_labels WHERE msg_id = $1",
					Args:  []interface{}{msg2.ID()},
					Count: 0,
				},
				SQLAssertion{
					SQL:   "select count(*) from msgs_msg_labels l JOIN msgs_msg m ON l.msg_id = m.id WHERE m.contact_id = $1",
					Args:  []interface{}{Bob},
					Count: 0,
				},
			},
		},
	}

	RunActionTestCases(t, tcs)
}
