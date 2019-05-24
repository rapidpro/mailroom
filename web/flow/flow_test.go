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
	"github.com/nyaruka/goflow/utils"

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

	server := web.NewServer(ctx, config.Mailroom, db, rp, nil, wg)
	server.Start()

	// give our server time to start
	time.Sleep(time.Second)

	defer server.Stop()
	defer utils.SetUUIDGenerator(utils.DefaultUUIDGenerator)

	tcs := []struct {
		URL          string
		Method       string
		BodyFile     string
		Status       int
		Response     string
		ResponseFile string
	}{
		{URL: "/mr/flow/migrate", Method: "GET", Status: 405, Response: `{"error": "illegal method: GET"}`},
		{URL: "/mr/flow/migrate", Method: "POST", BodyFile: "migrate_minimal_legacy.json", Status: 200, ResponseFile: "migrate_minimal_legacy.response.json"},
		{URL: "/mr/flow/validate", Method: "GET", Status: 405, Response: `{"error": "illegal method: GET"}`},
		{URL: "/mr/flow/validate", Method: "POST", BodyFile: "validate_valid_legacy.json", Status: 200, ResponseFile: "validate_valid_legacy.response.json"},
		{URL: "/mr/flow/validate", Method: "POST", BodyFile: "validate_invalid_legacy.json", Status: 422, ResponseFile: "validate_invalid_legacy.response.json"},
		{URL: "/mr/flow/validate", Method: "POST", BodyFile: "validate_valid.json", Status: 200, ResponseFile: "validate_valid.response.json"},
		{URL: "/mr/flow/validate", Method: "POST", BodyFile: "validate_invalid.json", Status: 422, ResponseFile: "validate_invalid.response.json"},
		{URL: "/mr/flow/validate", Method: "POST", BodyFile: "validate_valid_without_assets.json", Status: 200, ResponseFile: "validate_valid_without_assets.response.json"},
		{URL: "/mr/flow/validate", Method: "POST", BodyFile: "validate_legacy_single_msg.json", Status: 200, ResponseFile: "validate_legacy_single_msg.response.json"},
		{URL: "/mr/flow/clone", Method: "POST", BodyFile: "clone_valid.json", Status: 200, ResponseFile: "clone_valid.response.json"},
	}

	for _, tc := range tcs {
		utils.SetUUIDGenerator(utils.NewSeededUUID4Generator(12345))

		testID := fmt.Sprintf("%s %s %s", tc.Method, tc.URL, tc.BodyFile)
		var requestBody io.Reader
		var expectedRespBody []byte
		var err error

		if tc.BodyFile != "" {
			requestBody, err = os.Open("testdata/" + tc.BodyFile)
			require.NoError(t, err, "unable to open %s", tc.BodyFile)
		}
		if tc.ResponseFile != "" {
			expectedRespBody, err = ioutil.ReadFile("testdata/" + tc.ResponseFile)
			require.NoError(t, err, "unable to read %s", tc.ResponseFile)
		} else {
			expectedRespBody = []byte(tc.Response)
		}

		req, err := http.NewRequest(tc.Method, "http://localhost:8090"+tc.URL, requestBody)
		assert.NoError(t, err, "error creating request in %s", testID)

		resp, err := http.DefaultClient.Do(req)
		assert.NoError(t, err, "error making request in %s", testID)

		content, err := ioutil.ReadAll(resp.Body)
		require.NoError(t, err, "error reading body in %s", testID)

		assert.Equal(t, tc.Status, resp.StatusCode, "unexpected status in %s (response=%s)", testID, content)
		test.AssertEqualJSON(t, expectedRespBody, content, "response mismatch in %s", testID)
	}
}
