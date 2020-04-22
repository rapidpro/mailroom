package hooks

import (
	"testing"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/models"
)

func TestContactUnblocked(t *testing.T) {
	tcs := []HookEventsTestCase{
		HookEventsTestCase{
			Events: []flows.Event{events.NewContactUnblocked()},
			SQLAssertions: []SQLAssertion{
				SQLAssertion{
					SQL:   `select count(*) from contacts_contact where id = $1 AND is_blocked = FALSE`,
					Args:  []interface{}{models.CathyID},
					Count: 1,
				},
			},
		},
	}

	RunEventsTestCases(t, tcs)
}
