package hooks

import (
	"testing"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/models"
)

func TestContactStopped(t *testing.T) {
	tcs := []HookEventsTestCase{
		HookEventsTestCase{
			Events: []flows.Event{events.NewContactStopped()},
			SQLAssertions: []SQLAssertion{
				SQLAssertion{
					SQL:   `select count(*) from contacts_contact where id = $1 AND is_stopped = TRUE`,
					Args:  []interface{}{models.CathyID},
					Count: 1,
				},
			},
		},
	}

	RunEventsTestCases(t, tcs)
}
