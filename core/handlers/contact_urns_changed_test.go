package handlers_test

import (
	"testing"
	"time"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/actions"
	"github.com/nyaruka/mailroom/core/handlers"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
)

func TestContactURNsChanged(t *testing.T) {
	ctx, rt, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetAll)

	// add a URN to george that cathy will steal
	testdata.InsertContactURN(db, testdata.Org1, testdata.George, urns.URN("tel:+12065551212"), 100)

	now := time.Now()

	tcs := []handlers.TestCase{
		{
			Actions: handlers.ContactActionMap{
				testdata.Cathy: []flows.Action{
					actions.NewAddContactURN(handlers.NewActionUUID(), "tel", "12065551212"),
					actions.NewAddContactURN(handlers.NewActionUUID(), "tel", "12065551212"),
					actions.NewAddContactURN(handlers.NewActionUUID(), "telegram", "11551"),
					actions.NewAddContactURN(handlers.NewActionUUID(), "tel", "+16055741111"),
				},
				testdata.George: []flows.Action{},
			},
			SQLAssertions: []handlers.SQLAssertion{
				{
					SQL:   "select count(*) from contacts_contacturn where contact_id = $1 and scheme = 'telegram' and path = '11551' and priority = 998",
					Args:  []interface{}{testdata.Cathy.ID},
					Count: 1,
				},
				{
					SQL:   "select count(*) from contacts_contacturn where contact_id = $1 and scheme = 'tel' and path = '+12065551212' and priority = 999 and identity = 'tel:+12065551212'",
					Args:  []interface{}{testdata.Cathy.ID},
					Count: 1,
				},
				{
					SQL:   "select count(*) from contacts_contacturn where contact_id = $1 and scheme = 'tel' and path = '+16055741111' and priority = 1000",
					Args:  []interface{}{testdata.Cathy.ID},
					Count: 1,
				},
				// evan lost his 206 URN
				{
					SQL:   "select count(*) from contacts_contacturn where contact_id = $1",
					Args:  []interface{}{testdata.George.ID},
					Count: 1,
				},
				// two contacts updated, both cathy and evan since their URNs changed
				{
					SQL:   "select count(*) from contacts_contact where modified_on > $1",
					Args:  []interface{}{now},
					Count: 2,
				},
			},
		},
	}

	handlers.RunTestCases(t, ctx, rt, tcs)
}
