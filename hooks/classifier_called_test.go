package hooks

import (
	"testing"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/actions"
	"github.com/nyaruka/mailroom/models"
)

func TestClassifierCalled(t *testing.T) {
	wit := assets.NewClassifierReference(models.WitUUID, "Wit Classifier")

	tcs := []HookTestCase{
		HookTestCase{
			Actions: ContactActionMap{
				models.CathyID: []flows.Action{
					actions.NewCallClassifier(newActionUUID(), wit, "book me a flight", "flight"),
				},
			},
			SQLAssertions: []SQLAssertion{
				SQLAssertion{
					SQL:   `select count(*) from request_logs_httplog where org_id = $1 AND is_error = TRUE AND classifier_id = $2`,
					Args:  []interface{}{models.Org1, models.WitID},
					Count: 1,
				},
			},
		},
	}

	RunActionTestCases(t, tcs)
}
