package handlers_test

import (
	"testing"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/mailroom/core/handlers"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/actions"
)

func TestInputLabelsAdded(t *testing.T) {
	ctx, rt, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetAll)

	reporting := assets.NewLabelReference(assets.LabelUUID("ebc4dedc-91c4-4ed4-9dd6-daa05ea82698"), "Reporting")
	testing := assets.NewLabelReference(assets.LabelUUID("a6338cdc-7938-4437-8b05-2d5d785e3a08"), "Testing")

	msg1 := testdata.InsertIncomingMsg(db, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "start", models.MsgStatusHandled)
	msg2 := testdata.InsertIncomingMsg(db, testdata.Org1, testdata.TwilioChannel, testdata.Bob, "start", models.MsgStatusHandled)

	tcs := []handlers.TestCase{
		{
			Actions: handlers.ContactActionMap{
				testdata.Cathy: []flows.Action{
					actions.NewAddInputLabels(handlers.NewActionUUID(), []*assets.LabelReference{reporting}),
					actions.NewAddInputLabels(handlers.NewActionUUID(), []*assets.LabelReference{testing}),
					actions.NewAddInputLabels(handlers.NewActionUUID(), []*assets.LabelReference{reporting}),
				},
				testdata.Bob: []flows.Action{},
				testdata.George: []flows.Action{
					actions.NewAddInputLabels(handlers.NewActionUUID(), []*assets.LabelReference{testing}),
					actions.NewAddInputLabels(handlers.NewActionUUID(), []*assets.LabelReference{reporting}),
				},
			},
			Msgs: handlers.ContactMsgMap{
				testdata.Cathy: msg1,
				testdata.Bob:   msg2,
			},
			SQLAssertions: []handlers.SQLAssertion{
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
					Args:  []interface{}{testdata.Bob.ID},
					Count: 0,
				},
			},
		},
	}

	handlers.RunTestCases(t, ctx, rt, tcs)
}
