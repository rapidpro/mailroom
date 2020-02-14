package flow

import (
	"testing"

	"github.com/nyaruka/mailroom/testsuite"
)

func TestServer(t *testing.T) {
	tcs := []testsuite.ServerTestCase{
		{URL: "/mr/flow/migrate", Method: "GET", Status: 405, Response: `{"error": "illegal method: GET"}`},
		{URL: "/mr/flow/migrate", Method: "POST", Files: "migrate_minimal_v13", Status: 200},
		{URL: "/mr/flow/migrate", Method: "POST", Files: "migrate_minimal_legacy", Status: 200},
		{URL: "/mr/flow/migrate", Method: "POST", Files: "migrate_legacy_with_version", Status: 200},
		{URL: "/mr/flow/migrate", Method: "POST", Files: "migrate_invalid_v13", Status: 422},

		{URL: "/mr/flow/inspect", Method: "GET", Status: 405, Response: `{"error": "illegal method: GET"}`},
		{URL: "/mr/flow/inspect", Method: "POST", Files: "inspect_valid_legacy", Status: 200},
		{URL: "/mr/flow/inspect", Method: "POST", Files: "inspect_invalid_legacy", Status: 200},
		{URL: "/mr/flow/inspect", Method: "POST", Files: "inspect_valid", Status: 200},
		{URL: "/mr/flow/inspect", Method: "POST", Files: "inspect_invalid", Status: 422},
		{URL: "/mr/flow/inspect", Method: "POST", Files: "inspect_valid_without_org", Status: 200},
		{URL: "/mr/flow/inspect", Method: "POST", Files: "inspect_invalid_without_org", Status: 200},
		{URL: "/mr/flow/inspect", Method: "POST", Files: "inspect_legacy_single_msg", Status: 200},

		{URL: "/mr/flow/clone", Method: "GET", Status: 405, Response: `{"error": "illegal method: GET"}`},
		{URL: "/mr/flow/clone", Method: "POST", Files: "clone_valid", Status: 200},
		{URL: "/mr/flow/clone", Method: "POST", Files: "clone_struct_invalid", Status: 422},
		{URL: "/mr/flow/clone", Method: "POST", Files: "clone_missing_dep_mapping", Status: 200},
	}

	testsuite.RunServerTestCases(t, tcs)
}
