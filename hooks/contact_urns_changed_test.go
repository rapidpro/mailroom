package hooks

import (
	"testing"
	"time"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/actions"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/testsuite"
)

func TestAddContactURN(t *testing.T) {
	// add a URN to george that cathy will steal
	db := testsuite.DB()
	db.MustExec(
		`INSERT INTO contacts_contacturn(org_id, contact_id, scheme, path, identity, priority) 
								  VALUES(1, $1, 'tel', '+12065551212', 'tel:+12065551212', 100)`, models.GeorgeID)

	now := time.Now()

	tcs := []HookTestCase{
		HookTestCase{
			Actions: ContactActionMap{
				models.CathyID: []flows.Action{
					actions.NewAddContactURN(newActionUUID(), "tel", "12065551212"),
					actions.NewAddContactURN(newActionUUID(), "tel", "12065551212"),
					actions.NewAddContactURN(newActionUUID(), "telegram", "11551"),
					actions.NewAddContactURN(newActionUUID(), "tel", "+16055741111"),
				},
				models.GeorgeID: []flows.Action{},
			},
			SQLAssertions: []SQLAssertion{
				{
					SQL:   "select count(*) from contacts_contacturn where contact_id = $1 and scheme = 'telegram' and path = '11551' and priority = 998",
					Args:  []interface{}{models.CathyID},
					Count: 1,
				},
				{
					SQL:   "select count(*) from contacts_contacturn where contact_id = $1 and scheme = 'tel' and path = '+12065551212' and priority = 999 and identity = 'tel:+12065551212'",
					Args:  []interface{}{models.CathyID},
					Count: 1,
				},
				{
					SQL:   "select count(*) from contacts_contacturn where contact_id = $1 and scheme = 'tel' and path = '+16055741111' and priority = 1000",
					Args:  []interface{}{models.CathyID},
					Count: 1,
				},
				// evan lost his 206 URN
				{
					SQL:   "select count(*) from contacts_contacturn where contact_id = $1",
					Args:  []interface{}{models.GeorgeID},
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

	RunHookTestCases(t, tcs)
}
