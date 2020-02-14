package web

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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type ServerTestCase struct {
	URL      string
	Method   string
	Files    string
	Status   int
	Response string
}

func RunServerTestCases(t *testing.T, tcs []ServerTestCase) {
	testsuite.Reset()
	ctx := testsuite.CTX()
	db := testsuite.DB()
	rp := testsuite.RP()
	wg := &sync.WaitGroup{}

	server := NewServer(ctx, config.Mailroom, db, rp, nil, nil, wg)
	server.Start()

	// give our server time to start
	time.Sleep(time.Second)

	defer server.Stop()
	defer uuids.SetGenerator(uuids.DefaultGenerator)

	for _, tc := range tcs {
		uuids.SetGenerator(uuids.NewSeededGenerator(123456))
		time.Sleep(1 * time.Second)

		testID := fmt.Sprintf("%s %s %s", tc.Method, tc.URL, tc.Files)
		var requestBody io.Reader
		var err error

		if tc.Files != "" {
			requestPath := "testdata/" + tc.Files + ".json"
			requestBody, err = os.Open(requestPath)
			require.NoError(t, err, "unable to open %s", requestPath)
		}

		req, err := http.NewRequest(tc.Method, "http://localhost:8090"+tc.URL, requestBody)
		assert.NoError(t, err, "error creating request in %s", testID)

		resp, err := http.DefaultClient.Do(req)
		assert.NoError(t, err, "error making request in %s", testID)

		content, err := ioutil.ReadAll(resp.Body)
		require.NoError(t, err, "error reading body in %s", testID)

		assert.Equal(t, tc.Status, resp.StatusCode, "unexpected status in %s (response=%s)", testID, content)

		if tc.Files != "" {
			responsePath := "testdata/" + tc.Files + ".response.json"

			if !test.WriteOutput {
				expectedRespBody, err := ioutil.ReadFile(responsePath)
				require.NoError(t, err, "unable to read %s", responsePath)

				test.AssertEqualJSON(t, expectedRespBody, content, "response mismatch in %s", testID)
			} else {
				ioutil.WriteFile(responsePath, content, 0666)
			}
		} else {
			test.AssertEqualJSON(t, []byte(tc.Response), content, "response mismatch in %s", testID)
		}
	}
}
