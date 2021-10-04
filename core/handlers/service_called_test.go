package handlers_test

import (
	"testing"

	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/actions"
	"github.com/nyaruka/mailroom/core/handlers"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
)

func TestServiceCalled(t *testing.T) {
	ctx, rt, _, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetAll)
	defer httpx.SetRequestor(httpx.DefaultRequestor)

	httpx.SetRequestor(httpx.NewMockRequestor(map[string][]httpx.MockResponse{
		"https://api.wit.ai/message?v=20200513&q=book+me+a+flight": {
			httpx.NewMockResponse(200, nil, `{
				"text": "I want to book a flight to Quito",
				"intents": [
				  {
					"id": "754569408690533",
					"name": "book_flight",
					"confidence": 0.9024
				  }
				]
			}`),
		},
	}))

	wit := assets.NewClassifierReference(testdata.Wit.UUID, "Wit Classifier")

	tcs := []handlers.TestCase{
		{
			Actions: handlers.ContactActionMap{
				testdata.Cathy: []flows.Action{
					actions.NewCallClassifier(handlers.NewActionUUID(), wit, "book me a flight", "flight"),
				},
			},
			SQLAssertions: []handlers.SQLAssertion{
				{
					SQL:   `select count(*) from request_logs_httplog where org_id = $1 AND is_error = FALSE AND classifier_id = $2 AND url = 'https://api.wit.ai/message?v=20200513&q=book+me+a+flight'`,
					Args:  []interface{}{testdata.Org1.ID, testdata.Wit.ID},
					Count: 1,
				},
			},
		},
	}

	handlers.RunTestCases(t, ctx, rt, tcs)
}
