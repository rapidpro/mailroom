package handlers_test

import (
	"fmt"
	"testing"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/actions"
	"github.com/nyaruka/mailroom/core/handlers"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/nyaruka/redisx/assertredis"
)

func TestMsgCreated(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetAll)

	rt.Config.AttachmentDomain = "foo.bar.com"
	defer func() { rt.Config.AttachmentDomain = "" }()

	// add a URN for cathy so we can test all urn sends
	testdata.InsertContactURN(rt, testdata.Org1, testdata.Cathy, urns.URN("tel:+12065551212"), 10, nil)

	// delete all URNs for bob
	rt.DB.MustExec(`DELETE FROM contacts_contacturn WHERE contact_id = $1`, testdata.Bob.ID)

	// change alexandrias URN to a facebook URN and set her language to eng so that a template gets used for her
	rt.DB.MustExec(`UPDATE contacts_contacturn SET identity = 'facebook:12345', path='12345', scheme='facebook' WHERE contact_id = $1`, testdata.Alexandria.ID)
	rt.DB.MustExec(`UPDATE contacts_contact SET language='eng' WHERE id = $1`, testdata.Alexandria.ID)

	msg1 := testdata.InsertIncomingMsg(rt, testdata.Org1, testdata.TwilioChannel, testdata.Cathy, "start", models.MsgStatusHandled)

	templateAction := actions.NewSendMsg(handlers.NewActionUUID(), "Template time", nil, nil, false)
	templateAction.Templating = &actions.Templating{
		UUID:      uuids.UUID("db297d56-ec8c-4231-bbe8-030369777ae1"),
		Template:  &assets.TemplateReference{UUID: assets.TemplateUUID("9c22b594-fcab-4b29-9bcb-ce4404894a80"), Name: "revive_issue"},
		Variables: []string{"@contact.name", "tooth"},
	}

	tcs := []handlers.TestCase{
		{
			Actions: handlers.ContactActionMap{
				testdata.Cathy: []flows.Action{
					actions.NewSendMsg(handlers.NewActionUUID(), "Hello World", nil, []string{"yes", "no"}, true),
				},
				testdata.George: []flows.Action{
					actions.NewSendMsg(handlers.NewActionUUID(), "Hello Attachments", []string{"image/png:/images/image1.png"}, nil, true),
				},
				testdata.Bob: []flows.Action{
					actions.NewSendMsg(handlers.NewActionUUID(), "No URNs", nil, nil, false),
				},
				testdata.Alexandria: []flows.Action{
					templateAction,
				},
			},
			Msgs: handlers.ContactMsgMap{
				testdata.Cathy: msg1.FlowMsg,
			},
			SQLAssertions: []handlers.SQLAssertion{
				{
					SQL:   `SELECT COUNT(*) FROM msgs_msg WHERE text='Hello World' AND contact_id = $1 AND quick_replies[1] = 'yes' AND quick_replies[2] = 'no' AND high_priority = TRUE`,
					Args:  []any{testdata.Cathy.ID},
					Count: 2,
				},
				{
					SQL:   "SELECT COUNT(*) FROM msgs_msg WHERE text='Hello Attachments' AND contact_id = $1 AND attachments[1] = $2 AND status = 'Q' AND high_priority = FALSE",
					Args:  []any{testdata.George.ID, "image/png:https://foo.bar.com/images/image1.png"},
					Count: 1,
				},
				{
					SQL:   "SELECT COUNT(*) FROM msgs_msg WHERE contact_id=$1 AND STATUS = 'F' AND failed_reason = 'D';",
					Args:  []any{testdata.Bob.ID},
					Count: 1,
				},
				{
					SQL: "SELECT COUNT(*) FROM msgs_msg WHERE contact_id = $1 AND text = $2 AND direction = 'O' AND status = 'Q' AND channel_id = $3 AND metadata::jsonb->'templating'->'template'->>'name' = 'revive_issue'",
					Args: []any{
						testdata.Alexandria.ID,
						`Hi Alexandia, are you still experiencing problems with tooth?`,
						testdata.FacebookChannel.ID,
					},
					Count: 1,
				},
			},
		},
	}

	handlers.RunTestCases(t, ctx, rt, tcs)

	// Cathy should have 1 batch of queued messages at high priority
	assertredis.ZCard(t, rt.RP, fmt.Sprintf("msgs:%s|10/1", testdata.TwilioChannel.UUID), 1)

	// One bulk for George
	assertredis.ZCard(t, rt.RP, fmt.Sprintf("msgs:%s|10/0", testdata.TwilioChannel.UUID), 1)
}

func TestNewURN(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetAll)

	// switch our twitter channel to telegram
	telegramUUID := testdata.FacebookChannel.UUID
	telegramID := testdata.FacebookChannel.ID
	rt.DB.MustExec(
		`UPDATE channels_channel SET channel_type = 'TG', name = 'Telegram', schemes = ARRAY['telegram'] WHERE uuid = $1`,
		telegramUUID,
	)

	// give George a URN that Bob will steal
	testdata.InsertContactURN(rt, testdata.Org1, testdata.George, urns.URN("telegram:67890"), 1, nil)

	tcs := []handlers.TestCase{
		{
			Actions: handlers.ContactActionMap{
				// brand new URN on Cathy
				testdata.Cathy: []flows.Action{
					actions.NewAddContactURN(handlers.NewActionUUID(), "telegram", "12345"),
					actions.NewSetContactChannel(handlers.NewActionUUID(), assets.NewChannelReference(telegramUUID, "telegram")),
					actions.NewSendMsg(handlers.NewActionUUID(), "Cathy Message", nil, nil, false),
				},

				// Bob is stealing a URN previously assigned to George
				testdata.Bob: []flows.Action{
					actions.NewAddContactURN(handlers.NewActionUUID(), "telegram", "67890"),
					actions.NewSetContactChannel(handlers.NewActionUUID(), assets.NewChannelReference(telegramUUID, "telegram")),
					actions.NewSendMsg(handlers.NewActionUUID(), "Bob Message", nil, nil, false),
				},
			},
			SQLAssertions: []handlers.SQLAssertion{
				{
					SQL: `
					SELECT 
					  COUNT(*) 
					FROM 
					  msgs_msg m 
					  JOIN contacts_contacturn u ON m.contact_urn_id = u.id
					WHERE 
					  m.text='Cathy Message' AND 
					  m.contact_id = $1 AND 
					  m.status = 'Q' AND
					  u.identity = $2 AND
					  m.channel_id = $3 AND
					  u.channel_id IS NULL`,
					Args:  []any{testdata.Cathy.ID, "telegram:12345", telegramID},
					Count: 1,
				},
				{
					SQL: `
					SELECT 
					  COUNT(*) 
					FROM 
					  msgs_msg m 
					  JOIN contacts_contacturn u ON m.contact_urn_id = u.id
					WHERE 
					  m.text='Bob Message' AND 
					  m.contact_id = $1 AND 
					  m.status = 'Q' AND
					  u.identity = $2 AND
					  m.channel_id = $3 AND
					  u.channel_id IS NULL`,
					Args:  []any{testdata.Bob.ID, "telegram:67890", telegramID},
					Count: 1,
				},
			},
		},
	}

	handlers.RunTestCases(t, ctx, rt, tcs)
}
