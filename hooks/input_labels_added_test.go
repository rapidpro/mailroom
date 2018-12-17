package hooks

import (
	"testing"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/testsuite"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/actions"
)

func TestInputLabelsAdded(t *testing.T) {
	db := testsuite.DB()

	reporting := assets.NewLabelReference(assets.LabelUUID("981a4378-4c25-4ba4-bf46-74f357e7f058"), "Reporting")
	testing := assets.NewLabelReference(assets.LabelUUID("57ec7d9b-f00d-4bda-8db0-0037b5f50c8d"), "Testing")

	msg1 := createIncomingMsg(db, models.Org1, models.Cathy, models.CathyURN, models.CathyURNID, "start")
	msg2 := createIncomingMsg(db, models.Org1, models.Bob, models.BobURN, models.BobURNID, "start")

	tcs := []HookTestCase{
		HookTestCase{
			Actions: ContactActionMap{
				models.Cathy: []flows.Action{
					actions.NewAddInputLabelsAction(newActionUUID(), []*assets.LabelReference{reporting}),
					actions.NewAddInputLabelsAction(newActionUUID(), []*assets.LabelReference{testing}),
					actions.NewAddInputLabelsAction(newActionUUID(), []*assets.LabelReference{reporting}),
				},
				models.Bob: []flows.Action{},
				models.Evan: []flows.Action{
					actions.NewAddInputLabelsAction(newActionUUID(), []*assets.LabelReference{testing}),
					actions.NewAddInputLabelsAction(newActionUUID(), []*assets.LabelReference{reporting}),
				},
			},
			Msgs: ContactMsgMap{
				models.Cathy: msg1,
				models.Bob:   msg2,
			},
			SQLAssertions: []SQLAssertion{
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
					Args:  []interface{}{models.Bob},
					Count: 0,
				},
			},
		},
	}

	RunActionTestCases(t, tcs)
}
