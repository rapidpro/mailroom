package handlers_test

import (
	"testing"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/actions"
	"github.com/nyaruka/mailroom/core/handlers"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
)

func TestContactNameChanged(t *testing.T) {
	ctx, rt, _, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetAll)

	tcs := []handlers.TestCase{
		{
			Actions: handlers.ContactActionMap{
				testdata.Cathy: []flows.Action{
					actions.NewSetContactName(handlers.NewActionUUID(), "Fred"),
					actions.NewSetContactName(handlers.NewActionUUID(), "Tarzan"),
				},
				testdata.George: []flows.Action{
					actions.NewSetContactName(handlers.NewActionUUID(), "Geoff Newman"),
				},
				testdata.Bob: []flows.Action{
					actions.NewSetContactName(handlers.NewActionUUID(), ""),
				},
				testdata.Alexandria: []flows.Action{
					actions.NewSetContactName(handlers.NewActionUUID(), "😃234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"),
				},
			},
			SQLAssertions: []handlers.SQLAssertion{
				{
					SQL:   "select count(*) from contacts_contact where name = 'Tarzan' and id = $1",
					Args:  []interface{}{testdata.Cathy.ID},
					Count: 1,
				},
				{
					SQL:   "select count(*) from contacts_contact where name = 'Tarzan'",
					Count: 1,
				},
				{
					SQL:   "select count(*) from contacts_contact where name IS NULL and id = $1",
					Args:  []interface{}{testdata.Bob.ID},
					Count: 1,
				},
				{
					SQL:   "select count(*) from contacts_contact where name = 'Geoff Newman' and id = $1",
					Args:  []interface{}{testdata.George.ID},
					Count: 1,
				},
				{
					SQL:   "select count(*) from contacts_contact where name = '😃2345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678' and id = $1",
					Args:  []interface{}{testdata.Alexandria.ID},
					Count: 1,
				},
			},
		},
	}

	handlers.RunTestCases(t, ctx, rt, tcs)
}
