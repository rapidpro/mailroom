package handlers_test

import (
	"testing"

	"github.com/nyaruka/mailroom/core/handlers"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"

	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/actions"
)

func TestWebhookCalled(t *testing.T) {
	testsuite.Reset()

	defer httpx.SetRequestor(httpx.DefaultRequestor)

	httpx.SetRequestor(httpx.NewMockRequestor(map[string][]httpx.MockResponse{
		"http://rapidpro.io/": {
			httpx.NewMockResponse(200, nil, "OK"),
			httpx.NewMockResponse(200, nil, "OK"),
		},
		"http://rapidpro.io/?unsub=1": {
			httpx.NewMockResponse(410, nil, "Gone"),
			httpx.NewMockResponse(410, nil, "Gone"),
			httpx.NewMockResponse(410, nil, "Gone"),
		},
	}))

	// add a few resthooks
	testsuite.DB().MustExec(`INSERT INTO api_resthook(is_active, slug, org_id, created_on, modified_on, created_by_id, modified_by_id) VALUES(TRUE, 'foo', 1, NOW(), NOW(), 1, 1);`)
	testsuite.DB().MustExec(`INSERT INTO api_resthook(is_active, slug, org_id, created_on, modified_on, created_by_id, modified_by_id) VALUES(TRUE, 'bar', 1, NOW(), NOW(), 1, 1);`)

	// and a few targets
	testsuite.DB().MustExec(`INSERT INTO api_resthooksubscriber(is_active, created_on, modified_on, target_url, created_by_id, modified_by_id, resthook_id) VALUES(TRUE, NOW(), NOW(), 'http://rapidpro.io/', 1, 1, 1);`)
	testsuite.DB().MustExec(`INSERT INTO api_resthooksubscriber(is_active, created_on, modified_on, target_url, created_by_id, modified_by_id, resthook_id) VALUES(TRUE, NOW(), NOW(), 'http://rapidpro.io/?unsub=1', 1, 1, 2);`)
	testsuite.DB().MustExec(`INSERT INTO api_resthooksubscriber(is_active, created_on, modified_on, target_url, created_by_id, modified_by_id, resthook_id) VALUES(TRUE, NOW(), NOW(), 'http://rapidpro.io/?unsub=1', 1, 1, 1);`)

	tcs := []handlers.TestCase{
		{
			Actions: handlers.ContactActionMap{
				models.CathyID: []flows.Action{
					actions.NewCallResthook(handlers.NewActionUUID(), "foo", "foo"),
				},
				models.GeorgeID: []flows.Action{
					actions.NewCallResthook(handlers.NewActionUUID(), "foo", "foo"),
					actions.NewCallWebhook(handlers.NewActionUUID(), "GET", "http://rapidpro.io/?unsub=1", nil, "", ""),
				},
			},
			SQLAssertions: []handlers.SQLAssertion{
				{
					SQL:   "select count(*) from api_resthooksubscriber where is_active = FALSE",
					Args:  nil,
					Count: 1,
				},
				{
					SQL:   "select count(*) from api_resthooksubscriber where is_active = TRUE and resthook_id = $1",
					Args:  []interface{}{2},
					Count: 1,
				},
				{
					SQL:   "select count(*) from api_resthooksubscriber where is_active = TRUE",
					Args:  nil,
					Count: 2,
				},
				{
					SQL:   "select count(*) from api_webhookresult where contact_id = $1 AND status_code = 200",
					Args:  []interface{}{models.CathyID},
					Count: 1,
				},
				{
					SQL:   "select count(*) from api_webhookresult where contact_id = $1 AND status_code = 410",
					Args:  []interface{}{models.CathyID},
					Count: 1,
				},
				{
					SQL:   "select count(*) from api_webhookresult where contact_id = $1",
					Args:  []interface{}{models.GeorgeID},
					Count: 3,
				},
				{
					SQL:   "select count(*) from api_webhookevent where org_id = $1",
					Args:  []interface{}{models.Org1},
					Count: 2,
				},
			},
		},
	}

	handlers.RunTestCases(t, tcs)
}
