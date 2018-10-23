package hooks

import (
	"testing"
	"time"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/actions"
	"github.com/nyaruka/mailroom/testsuite"
)

func TestAddContactURN(t *testing.T) {
	// add a URN to evan that cathy will steal
	db := testsuite.DB()
	db.MustExec(
		`INSERT INTO contacts_contacturn(org_id, contact_id, scheme, path, identity, priority) 
								  VALUES(1, $1, 'tel', '+12065551212', 'tel:+12065551212', 100)`, Evan)

	now := time.Now()

	tcs := []HookTestCase{
		HookTestCase{
			Actions: ContactActionMap{
				Cathy: []flows.Action{
					actions.NewAddContactURNAction(newActionUUID(), "tel", "12065551212"),
					actions.NewAddContactURNAction(newActionUUID(), "tel", "12065551212"),
					actions.NewAddContactURNAction(newActionUUID(), "telegram", "11551"),
					actions.NewAddContactURNAction(newActionUUID(), "tel", "250700000002"),
				},
				Evan: []flows.Action{},
			},
			Assertions: []SQLAssertion{
				SQLAssertion{
					SQL:   "select count(*) from contacts_contacturn where contact_id = $1 and scheme = 'telegram' and path = '11551' and priority = 998",
					Args:  []interface{}{Cathy},
					Count: 1,
				},
				SQLAssertion{
					SQL:   "select count(*) from contacts_contacturn where contact_id = $1 and scheme = 'tel' and path = '+12065551212' and priority = 999 and identity = 'tel:+12065551212'",
					Args:  []interface{}{Cathy},
					Count: 1,
				},
				SQLAssertion{
					SQL:   "select count(*) from contacts_contacturn where contact_id = $1 and scheme = 'tel' and path = '+250700000002' and priority = 1000",
					Args:  []interface{}{Cathy},
					Count: 1,
				},
				// evan lost his 206 URN
				SQLAssertion{
					SQL:   "select count(*) from contacts_contacturn where contact_id = $1",
					Args:  []interface{}{Evan},
					Count: 1,
				},
				// two contacts updated, both cathy and evan since their URNs changed
				SQLAssertion{
					SQL:   "select count(*) from contacts_contact where modified_on > $1",
					Args:  []interface{}{now},
					Count: 2,
				},
			},
		},
	}

	RunActionTestCases(t, tcs)
}
