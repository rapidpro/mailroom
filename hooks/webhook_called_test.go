package hooks

import (
	"testing"

	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/testsuite"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/actions"
	"github.com/nyaruka/goflow/utils/httpx"
)

func TestWebhookCalled(t *testing.T) {
	testsuite.Reset()

	defer httpx.SetRequestor(httpx.DefaultRequestor)

	httpx.SetRequestor(httpx.NewMockRequestor(map[string][]httpx.MockResponse{
		"http://rapidpro.io/": []httpx.MockResponse{
			httpx.NewMockResponse(200, nil, "OK"),
			httpx.NewMockResponse(200, nil, "OK"),
		},
		"http://rapidpro.io/?unsub=1": []httpx.MockResponse{
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

	tcs := []HookTestCase{
		HookTestCase{
			Actions: ContactActionMap{
				models.CathyID: []flows.Action{
					actions.NewCallResthook(newActionUUID(), "foo", "foo"),
				},
				models.GeorgeID: []flows.Action{
					actions.NewCallResthook(newActionUUID(), "foo", "foo"),
					actions.NewCallWebhook(newActionUUID(), "GET", "http://rapidpro.io/?unsub=1", nil, "", ""),
				},
			},
			SQLAssertions: []SQLAssertion{
				SQLAssertion{
					SQL:   "select count(*) from api_resthooksubscriber where is_active = FALSE",
					Args:  nil,
					Count: 1,
				},
				SQLAssertion{
					SQL:   "select count(*) from api_resthooksubscriber where is_active = TRUE and resthook_id = $1",
					Args:  []interface{}{2},
					Count: 1,
				},
				SQLAssertion{
					SQL:   "select count(*) from api_resthooksubscriber where is_active = TRUE",
					Args:  nil,
					Count: 2,
				},
				SQLAssertion{
					SQL:   "select count(*) from api_webhookresult where contact_id = $1 AND status_code = 200",
					Args:  []interface{}{models.CathyID},
					Count: 1,
				},
				SQLAssertion{
					SQL:   "select count(*) from api_webhookresult where contact_id = $1 AND status_code = 410",
					Args:  []interface{}{models.CathyID},
					Count: 1,
				},
				SQLAssertion{
					SQL:   "select count(*) from api_webhookresult where contact_id = $1",
					Args:  []interface{}{models.GeorgeID},
					Count: 3,
				},
				SQLAssertion{
					SQL:   "select count(*) from api_webhookevent where org_id = $1",
					Args:  []interface{}{models.Org1},
					Count: 2,
				},
			},
		},
	}

	RunHookTestCases(t, tcs)
}
