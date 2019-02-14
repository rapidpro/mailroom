package hooks

import (
	"testing"

	"github.com/nyaruka/mailroom/config"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/testsuite"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/actions"
)

func TestMsgCreated(t *testing.T) {
	testsuite.Reset()
	db := testsuite.DB()

	config.Mailroom.AttachmentDomain = "foo.bar.com"
	defer func() { config.Mailroom.AttachmentDomain = "" }()

	// add a URN for cathy so we can test all urn sends
	db.MustExec(
		`INSERT INTO contacts_contacturn(identity, path, scheme, priority, contact_id, org_id) 
		                          VALUES('tel:12065551212', '12065551212', 'tel', 10, $1, 1)`,
		models.CathyID)

	// delete all URNs for bob
	db.MustExec(`DELETE FROM contacts_contacturn WHERE contact_id = $1`, models.BobID)

	// TODO: test replying to a newly added URN

	msg1 := createIncomingMsg(db, models.Org1, models.CathyID, models.CathyURN, models.CathyURNID, "start")

	tcs := []HookTestCase{
		HookTestCase{
			Actions: ContactActionMap{
				models.CathyID: []flows.Action{
					actions.NewSendMsgAction(newActionUUID(), "Hello World", nil, []string{"yes", "no"}, true),
				},
				models.GeorgeID: []flows.Action{
					actions.NewSendMsgAction(newActionUUID(), "Hello Attachments", []string{"image/png:/images/image1.png"}, nil, true),
				},
				models.BobID: []flows.Action{
					actions.NewSendMsgAction(newActionUUID(), "No URNs", nil, nil, false),
				},
			},
			Msgs: ContactMsgMap{
				models.CathyID: msg1,
			},
			SQLAssertions: []SQLAssertion{
				SQLAssertion{
					SQL:   "SELECT COUNT(*) FROM msgs_msg WHERE text='Hello World' AND contact_id = $1 AND metadata = $2 AND response_to_id = $3",
					Args:  []interface{}{models.CathyID, `{"quick_replies":["yes","no"]}`, msg1.ID()},
					Count: 2,
				},
				SQLAssertion{
					SQL:   "SELECT COUNT(*) FROM msgs_msg WHERE text='Hello Attachments' AND contact_id = $1 AND attachments[1] = $2 AND status = 'Q'",
					Args:  []interface{}{models.GeorgeID, "image/png:https://foo.bar.com/images/image1.png"},
					Count: 1,
				},
				SQLAssertion{
					SQL:   "SELECT COUNT(*) FROM msgs_msg WHERE contact_id=$1;",
					Args:  []interface{}{models.BobID},
					Count: 0,
				},
			},
		},
	}

	RunActionTestCases(t, tcs)
}

func TestNoTopup(t *testing.T) {
	testsuite.Reset()
	db := testsuite.DB()

	// no more credits
	db.MustExec(`UPDATE orgs_topup SET credits = 0 WHERE org_id = $1`, models.Org1)

	tcs := []HookTestCase{
		HookTestCase{
			Actions: ContactActionMap{
				models.CathyID: []flows.Action{
					actions.NewSendMsgAction(newActionUUID(), "No Topup", nil, nil, false),
				},
			},
			SQLAssertions: []SQLAssertion{
				SQLAssertion{
					SQL:   "SELECT COUNT(*) FROM msgs_msg WHERE text='No Topup' AND contact_id = $1 AND status = 'P'",
					Args:  []interface{}{models.CathyID},
					Count: 1,
				},
			},
		},
	}

	RunActionTestCases(t, tcs)
}
