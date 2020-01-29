package flow

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/nyaruka/goflow/test"
	"github.com/nyaruka/goflow/utils/uuids"
	"github.com/nyaruka/mailroom/config"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/web"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServer(t *testing.T) {
	testsuite.Reset()
	ctx := testsuite.CTX()
	db := testsuite.DB()
	rp := testsuite.RP()
	wg := &sync.WaitGroup{}

	server := web.NewServer(ctx, config.Mailroom, db, rp, nil, nil, wg)
	server.Start()

	// give our server time to start
	time.Sleep(time.Second)

	defer server.Stop()
	defer uuids.SetGenerator(uuids.DefaultGenerator)

	tcs := []struct {
		URL             string
		Method          string
		BodyFile        string
		Status          int
		Response        string
		ResponseFile    string
		ResponsePattern string
	}{
		{URL: "/mr/flow/migrate", Method: "GET", Status: 405, Response: `{"error": "illegal method: GET"}`},
		{URL: "/mr/flow/migrate", Method: "POST", BodyFile: "migrate_minimal_v13.json", Status: 200, ResponseFile: "migrate_minimal_v13.response.json"},
		{URL: "/mr/flow/migrate", Method: "POST", BodyFile: "migrate_minimal_legacy.json", Status: 200, ResponseFile: "migrate_minimal_legacy.response.json"},
		{URL: "/mr/flow/migrate", Method: "POST", BodyFile: "migrate_legacy_with_version.json", Status: 200, ResponseFile: "migrate_legacy_with_version.response.json"},
		{URL: "/mr/flow/migrate", Method: "POST", BodyFile: "migrate_invalid_v13.json", Status: 422, Response: `{"error": "unable to read migrated flow: unable to read node: field 'uuid' is required"}`},

		{URL: "/mr/flow/inspect", Method: "GET", Status: 405, Response: `{"error": "illegal method: GET"}`},
		{URL: "/mr/flow/inspect", Method: "POST", BodyFile: "inspect_valid_legacy.json", Status: 200, ResponseFile: "inspect_valid_legacy.response.json"},
		{URL: "/mr/flow/inspect", Method: "POST", BodyFile: "inspect_invalid_legacy.json", Status: 422, ResponseFile: "inspect_invalid_legacy.response.json"},
		{URL: "/mr/flow/inspect", Method: "POST", BodyFile: "inspect_valid.json", Status: 200, ResponseFile: "inspect_valid.response.json"},
		{URL: "/mr/flow/inspect", Method: "POST", BodyFile: "inspect_invalid.json", Status: 422, ResponseFile: "inspect_invalid.response.json"},
		{URL: "/mr/flow/inspect", Method: "POST", BodyFile: "inspect_valid_without_org.json", Status: 200, ResponseFile: "inspect_valid_without_org.response.json"},
		{URL: "/mr/flow/inspect", Method: "POST", BodyFile: "inspect_invalid_without_org.json", Status: 200, ResponseFile: "inspect_invalid_without_org.response.json"},
		{URL: "/mr/flow/inspect", Method: "POST", BodyFile: "inspect_legacy_single_msg.json", Status: 200, ResponseFile: "inspect_legacy_single_msg.response.json"},

		{URL: "/mr/flow/clone", Method: "GET", Status: 405, Response: `{"error": "illegal method: GET"}`},
		{URL: "/mr/flow/clone", Method: "POST", BodyFile: "clone_valid.json", Status: 200, ResponsePattern: `"uuid": "1cf84575-ee14-4253-88b6-e3675c04a066"`},
		{URL: "/mr/flow/clone", Method: "POST", BodyFile: "clone_struct_invalid.json", Status: 422, Response: `{"error": "unable to clone flow: unable to read node: field 'uuid' is required"}`},
		{URL: "/mr/flow/clone", Method: "POST", BodyFile: "clone_missing_dep_mapping.json", Status: 422, ResponsePattern: `group\[uuid=[-0-9a-f]{36},name=Testers\]`},
		{URL: "/mr/flow/clone", Method: "POST", BodyFile: "clone_valid_bad_org.json", Status: 500, Response: `{"error": "error loading environment for org 167733: no org with id: 167733"}`},
	}

	for _, tc := range tcs {
		uuids.SetGenerator(uuids.NewSeededGenerator(123456))
		time.Sleep(1 * time.Second)

		testID := fmt.Sprintf("%s %s %s", tc.Method, tc.URL, tc.BodyFile)
		var requestBody io.Reader
		var err error

		if tc.BodyFile != "" {
			requestBody, err = os.Open("testdata/" + tc.BodyFile)
			require.NoError(t, err, "unable to open %s", tc.BodyFile)
		}

		req, err := http.NewRequest(tc.Method, "http://localhost:8090"+tc.URL, requestBody)
		assert.NoError(t, err, "error creating request in %s", testID)

		resp, err := http.DefaultClient.Do(req)
		assert.NoError(t, err, "error making request in %s", testID)

		content, err := ioutil.ReadAll(resp.Body)
		require.NoError(t, err, "error reading body in %s", testID)

		assert.Equal(t, tc.Status, resp.StatusCode, "unexpected status in %s (response=%s)", testID, content)

		if tc.ResponseFile != "" {
			expectedRespBody, err := ioutil.ReadFile("testdata/" + tc.ResponseFile)
			require.NoError(t, err, "unable to read %s", tc.ResponseFile)

			test.AssertEqualJSON(t, expectedRespBody, content, "response mismatch in %s", testID)
		} else if tc.ResponsePattern != "" {
			assert.Regexp(t, tc.ResponsePattern, string(content), "response mismatch in %s", testID)
		} else {
			test.AssertEqualJSON(t, []byte(tc.Response), content, "response mismatch in %s", testID)
		}
	}
}
