package hooks

import (
	"testing"

	"github.com/greatnonprofits-nfp/goflow/assets"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/testsuite"

	"github.com/greatnonprofits-nfp/goflow/flows"
	"github.com/greatnonprofits-nfp/goflow/flows/actions"
)

func TestInputLabelsAdded(t *testing.T) {
	db := testsuite.DB()

	reporting := assets.NewLabelReference(assets.LabelUUID("ebc4dedc-91c4-4ed4-9dd6-daa05ea82698"), "Reporting")
	testing := assets.NewLabelReference(assets.LabelUUID("a6338cdc-7938-4437-8b05-2d5d785e3a08"), "Testing")

	msg1 := createIncomingMsg(db, models.Org1, models.CathyID, models.CathyURN, models.CathyURNID, "start")
	msg2 := createIncomingMsg(db, models.Org1, models.BobID, models.BobURN, models.BobURNID, "start")

	tcs := []HookTestCase{
		HookTestCase{
			Actions: ContactActionMap{
				models.CathyID: []flows.Action{
					actions.NewAddInputLabelsAction(newActionUUID(), []*assets.LabelReference{reporting}),
					actions.NewAddInputLabelsAction(newActionUUID(), []*assets.LabelReference{testing}),
					actions.NewAddInputLabelsAction(newActionUUID(), []*assets.LabelReference{reporting}),
				},
				models.BobID: []flows.Action{},
				models.GeorgeID: []flows.Action{
					actions.NewAddInputLabelsAction(newActionUUID(), []*assets.LabelReference{testing}),
					actions.NewAddInputLabelsAction(newActionUUID(), []*assets.LabelReference{reporting}),
				},
			},
			Msgs: ContactMsgMap{
				models.CathyID: msg1,
				models.BobID:   msg2,
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
					Args:  []interface{}{models.BobID},
					Count: 0,
				},
			},
		},
	}

	RunActionTestCases(t, tcs)
}
