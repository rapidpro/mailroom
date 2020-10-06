package hooks_test

import (
	"testing"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/mailroom/hooks"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/actions"
)

func TestInputLabelsAdded(t *testing.T) {
	db := testsuite.DB()

	reporting := assets.NewLabelReference(assets.LabelUUID("ebc4dedc-91c4-4ed4-9dd6-daa05ea82698"), "Reporting")
	testing := assets.NewLabelReference(assets.LabelUUID("a6338cdc-7938-4437-8b05-2d5d785e3a08"), "Testing")

	msg1 := testdata.InsertIncomingMsg(t, db, models.Org1, models.CathyID, models.CathyURN, models.CathyURNID, "start")
	msg2 := testdata.InsertIncomingMsg(t, db, models.Org1, models.BobID, models.BobURN, models.BobURNID, "start")

	tcs := []hooks.TestCase{
		{
			Actions: hooks.ContactActionMap{
				models.CathyID: []flows.Action{
					actions.NewAddInputLabels(hooks.NewActionUUID(), []*assets.LabelReference{reporting}),
					actions.NewAddInputLabels(hooks.NewActionUUID(), []*assets.LabelReference{testing}),
					actions.NewAddInputLabels(hooks.NewActionUUID(), []*assets.LabelReference{reporting}),
				},
				models.BobID: []flows.Action{},
				models.GeorgeID: []flows.Action{
					actions.NewAddInputLabels(hooks.NewActionUUID(), []*assets.LabelReference{testing}),
					actions.NewAddInputLabels(hooks.NewActionUUID(), []*assets.LabelReference{reporting}),
				},
			},
			Msgs: hooks.ContactMsgMap{
				models.CathyID: msg1,
				models.BobID:   msg2,
			},
			SQLAssertions: []hooks.SQLAssertion{
				{
					SQL:   "select count(*) from msgs_msg_labels WHERE msg_id = $1",
					Args:  []interface{}{msg1.ID()},
					Count: 2,
				},
				{
					SQL:   "select count(*) from msgs_msg_labels WHERE msg_id = $1",
					Args:  []interface{}{msg2.ID()},
					Count: 0,
				},
				{
					SQL:   "select count(*) from msgs_msg_labels l JOIN msgs_msg m ON l.msg_id = m.id WHERE m.contact_id = $1",
					Args:  []interface{}{models.BobID},
					Count: 0,
				},
			},
		},
	}

	hooks.RunTestCases(t, tcs)
}
