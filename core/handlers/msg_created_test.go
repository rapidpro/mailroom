package handlers_test

import (
	"fmt"
	"testing"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/actions"
	"github.com/nyaruka/mailroom/config"
	"github.com/nyaruka/mailroom/core/handlers"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"

	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/assert"
)

func TestMsgCreated(t *testing.T) {
	testsuite.Reset()
	db := testsuite.DB()

	config.Mailroom.AttachmentDomain = "foo.bar.com"
	defer func() { config.Mailroom.AttachmentDomain = "" }()

	// add a URN for cathy so we can test all urn sends
	testdata.InsertContactURN(t, db, models.Org1, models.CathyID, urns.URN("tel:+12065551212"), 10)

	// delete all URNs for bob
	db.MustExec(`DELETE FROM contacts_contacturn WHERE contact_id = $1`, models.BobID)

	// change alexandrias URN to a twitter URN and set her language to eng so that a template gets used for her
	db.MustExec(`UPDATE contacts_contacturn SET identity = 'twitter:12345', path='12345', scheme='twitter' WHERE contact_id = $1`, models.AlexandriaID)
	db.MustExec(`UPDATE contacts_contact SET language='eng' WHERE id = $1`, models.AlexandriaID)

	msg1 := testdata.InsertIncomingMsg(t, db, models.Org1, models.CathyID, models.CathyURN, models.CathyURNID, "start")

	templateAction := actions.NewSendMsg(handlers.NewActionUUID(), "Template time", nil, nil, false)
	templateAction.Templating = &actions.Templating{
		UUID:      uuids.UUID("db297d56-ec8c-4231-bbe8-030369777ae1"),
		Template:  &assets.TemplateReference{UUID: assets.TemplateUUID("9c22b594-fcab-4b29-9bcb-ce4404894a80"), Name: "revive_issue"},
		Variables: []string{"@contact.name", "tooth"},
	}

	tcs := []handlers.TestCase{
		{
			Actions: handlers.ContactActionMap{
				models.CathyID: []flows.Action{
					actions.NewSendMsg(handlers.NewActionUUID(), "Hello World", nil, []string{"yes", "no"}, true),
				},
				models.GeorgeID: []flows.Action{
					actions.NewSendMsg(handlers.NewActionUUID(), "Hello Attachments", []string{"image/png:/images/image1.png"}, nil, true),
				},
				models.BobID: []flows.Action{
					actions.NewSendMsg(handlers.NewActionUUID(), "No URNs", nil, nil, false),
				},
				models.AlexandriaID: []flows.Action{
					templateAction,
				},
			},
			Msgs: handlers.ContactMsgMap{
				models.CathyID: msg1,
			},
			SQLAssertions: []handlers.SQLAssertion{
				{
					SQL:   "SELECT COUNT(*) FROM msgs_msg WHERE text='Hello World' AND contact_id = $1 AND metadata = $2 AND response_to_id = $3 AND high_priority = TRUE",
					Args:  []interface{}{models.CathyID, `{"quick_replies":["yes","no"]}`, msg1.ID()},
					Count: 2,
				},
				{
					SQL:   "SELECT COUNT(*) FROM msgs_msg WHERE text='Hello Attachments' AND contact_id = $1 AND attachments[1] = $2 AND status = 'Q' AND high_priority = FALSE",
					Args:  []interface{}{models.GeorgeID, "image/png:https://foo.bar.com/images/image1.png"},
					Count: 1,
				},
				{
					SQL:   "SELECT COUNT(*) FROM msgs_msg WHERE contact_id=$1;",
					Args:  []interface{}{models.BobID},
					Count: 0,
				},
				{
					SQL: "SELECT COUNT(*) FROM msgs_msg WHERE contact_id = $1 AND text = $2 AND metadata = $3 AND direction = 'O' AND status = 'Q' AND channel_id = $4",
					Args: []interface{}{
						models.AlexandriaID,
						`Hi Alexandia, are you still experiencing problems with tooth?`,
						`{"templating":{"template":{"uuid":"9c22b594-fcab-4b29-9bcb-ce4404894a80","name":"revive_issue"},"language":"eng","country":"US","variables":["Alexandia","tooth"]}}`,
						models.TwitterChannelID,
					},
					Count: 1,
				},
			},
		},
	}

	handlers.RunTestCases(t, tcs)

	rc := testsuite.RP().Get()
	defer rc.Close()

	// Cathy should have 1 batch of queued messages at high priority
	count, err := redis.Int(rc.Do("zcard", fmt.Sprintf("msgs:%s|10/1", models.TwilioChannelUUID)))
	assert.NoError(t, err)
	assert.Equal(t, 1, count)

	// One bulk for George
	count, err = redis.Int(rc.Do("zcard", fmt.Sprintf("msgs:%s|10/0", models.TwilioChannelUUID)))
	assert.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestNoTopup(t *testing.T) {
	testsuite.Reset()
	db := testsuite.DB()

	// no more credits
	db.MustExec(`UPDATE orgs_topup SET credits = 0 WHERE org_id = $1`, models.Org1)

	tcs := []handlers.TestCase{
		{
			Actions: handlers.ContactActionMap{
				models.CathyID: []flows.Action{
					actions.NewSendMsg(handlers.NewActionUUID(), "No Topup", nil, nil, false),
				},
			},
			SQLAssertions: []handlers.SQLAssertion{
				{
					SQL:   "SELECT COUNT(*) FROM msgs_msg WHERE text='No Topup' AND contact_id = $1 AND status = 'Q'",
					Args:  []interface{}{models.CathyID},
					Count: 1,
				},
			},
		},
	}

	handlers.RunTestCases(t, tcs)
}

func TestNewURN(t *testing.T) {
	testsuite.Reset()
	db := testsuite.DB()

	// switch our twitter channel to telegram
	telegramUUID := models.TwitterChannelUUID
	telegramID := models.TwitterChannelID
	db.MustExec(
		`UPDATE channels_channel SET channel_type = 'TG', name = 'Telegram', schemes = ARRAY['telegram'] WHERE uuid = $1`,
		telegramUUID,
	)

	// give George a URN that Bob will steal
	testdata.InsertContactURN(t, db, models.Org1, models.GeorgeID, urns.URN("telegram:67890"), 1)

	tcs := []handlers.TestCase{
		{
			Actions: handlers.ContactActionMap{
				// brand new URN on Cathy
				models.CathyID: []flows.Action{
					actions.NewAddContactURN(handlers.NewActionUUID(), "telegram", "12345"),
					actions.NewSetContactChannel(handlers.NewActionUUID(), assets.NewChannelReference(telegramUUID, "telegram")),
					actions.NewSendMsg(handlers.NewActionUUID(), "Cathy Message", nil, nil, false),
				},

				// Bob is stealing a URN previously assigned to George
				models.BobID: []flows.Action{
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
					Args:  []interface{}{models.CathyID, "telegram:12345", telegramID},
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
					Args:  []interface{}{models.BobID, "telegram:67890", telegramID},
					Count: 1,
				},
			},
		},
	}

	handlers.RunTestCases(t, tcs)
}
