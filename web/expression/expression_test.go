package expression_test

import (
	"testing"

	"github.com/nyaruka/mailroom/web"
)

func TestServer(t *testing.T) {
	tcs := []web.ServerTestCase{
		{URL: "/mr/expression/migrate", Method: "GET", Status: 405, Response: `{"error": "illegal method: GET"}`},
		{URL: "/mr/expression/migrate", Method: "POST", Files: "migrate_valid1", Status: 200},
		{URL: "/mr/expression/migrate", Method: "POST", Files: "migrate_valid2", Status: 200},
		{URL: "/mr/expression/migrate", Method: "POST", Files: "migrate_invalid", Status: 422},
	}

	web.RunServerTestCases(t, tcs)
}
