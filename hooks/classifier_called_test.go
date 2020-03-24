package hooks

import (
	"testing"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/actions"
	"github.com/nyaruka/goflow/utils/httpx"
	"github.com/nyaruka/mailroom/models"
)

func TestClassifierCalled(t *testing.T) {
	defer httpx.SetRequestor(httpx.DefaultRequestor)

	httpx.SetRequestor(httpx.NewMockRequestor(map[string][]httpx.MockResponse{
		"https://api.wit.ai/message?v=20170307&q=book+me+a+flight": []httpx.MockResponse{
			httpx.NewMockResponse(200, nil, `{"_text":"book me a flight","entities":{"intent":[{"confidence":0.84709152161066,"value":"book_flight"}]},"msg_id":"1M7fAcDWag76OmgDI"}`, 1),
		},
	}))

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
					SQL:   `select count(*) from request_logs_httplog where org_id = $1 AND is_error = FALSE AND classifier_id = $2 AND url = 'https://api.wit.ai/message?v=20170307&q=book+me+a+flight'`,
					Args:  []interface{}{models.Org1, models.WitID},
					Count: 1,
				},
			},
		},
	}

	RunActionTestCases(t, tcs)
}
