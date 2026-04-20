package handlers_test

import (
	"fmt"
	"testing"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/actions"
	"github.com/nyaruka/mailroom/core/handlers"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/nyaruka/redisx/assertredis"
	"github.com/stretchr/testify/assert"
)

func TestOptinRequested(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetAll)

	optIn := testdata.InsertOptIn(rt, testdata.Org1, "Jokes")
	models.FlushCache()

	rt.DB.MustExec(`UPDATE contacts_contacturn SET identity = 'facebook:12345', scheme='facebook', path='12345' WHERE contact_id = $1`, testdata.Cathy.ID)
	rt.DB.MustExec(`UPDATE contacts_contacturn SET identity = 'facebook:23456', scheme='facebook', path='23456' WHERE contact_id = $1`, testdata.George.ID)

	msg1 := testdata.InsertIncomingMsg(rt, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "start", models.MsgStatusHandled)

	oa := testdata.Org1.Load(rt)
	ch := oa.ChannelByUUID("0f661e8b-ea9d-4bd3-9953-d368340acf91")
	assert.Equal(t, models.ChannelType("FBA"), ch.Type())
	assert.Equal(t, []assets.ChannelFeature{assets.ChannelFeatureOptIns}, ch.Features())

	tcs := []handlers.TestCase{
		{
			Actions: handlers.ContactActionMap{
				testdata.Cathy: []flows.Action{
					actions.NewRequestOptIn(handlers.NewActionUUID(), assets.NewOptInReference(optIn.UUID, "Jokes")),
				},
				testdata.George: []flows.Action{
					actions.NewRequestOptIn(handlers.NewActionUUID(), assets.NewOptInReference(optIn.UUID, "Jokes")),
				},
				testdata.Bob: []flows.Action{
					actions.NewRequestOptIn(handlers.NewActionUUID(), assets.NewOptInReference(optIn.UUID, "Jokes")),
				},
			},
			Msgs: handlers.ContactMsgMap{
				testdata.Cathy: msg1.FlowMsg,
			},
			SQLAssertions: []handlers.SQLAssertion{
				{
					SQL:   `SELECT COUNT(*) FROM msgs_msg WHERE direction = 'O' AND text = '' AND high_priority = true AND contact_id = $1 AND optin_id = $2`,
					Args:  []any{testdata.Cathy.ID, optIn.ID},
					Count: 1,
				},
				{
					SQL:   `SELECT COUNT(*) FROM msgs_msg WHERE direction = 'O' AND text = '' AND high_priority = false AND contact_id = $1 AND optin_id = $2`,
					Args:  []any{testdata.George.ID, optIn.ID},
					Count: 1,
				},
				{ // bob has no channel+URN that supports optins
					SQL:   `SELECT COUNT(*) FROM msgs_msg WHERE direction = 'O' AND contact_id = $1`,
					Args:  []any{testdata.Bob.ID},
					Count: 0,
				},
			},
		},
	}

	handlers.RunTestCases(t, ctx, rt, tcs)

	// Cathy should have 1 batch of queued messages at high priority
	assertredis.ZCard(t, rt.RP, fmt.Sprintf("msgs:%s|10/1", testdata.FacebookChannel.UUID), 1)

	// One bulk for George
	assertredis.ZCard(t, rt.RP, fmt.Sprintf("msgs:%s|10/0", testdata.FacebookChannel.UUID), 1)
}
