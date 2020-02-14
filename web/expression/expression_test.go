package expression

import (
	"testing"

	"github.com/nyaruka/mailroom/testsuite"
)

func TestServer(t *testing.T) {
	tcs := []testsuite.ServerTestCase{
		{URL: "/mr/expression/migrate", Method: "GET", Status: 405, Response: `{"error": "illegal method: GET"}`},
		{URL: "/mr/expression/migrate", Method: "POST", Files: "migrate_valid1", Status: 200},
		{URL: "/mr/expression/migrate", Method: "POST", Files: "migrate_valid2", Status: 200},
		{URL: "/mr/expression/migrate", Method: "POST", Files: "migrate_invalid", Status: 422},
	}

	testsuite.RunServerTestCases(t, tcs)
}
