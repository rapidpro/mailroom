package web

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/nyaruka/goflow/test"
	"github.com/nyaruka/goflow/utils/dates"
	"github.com/nyaruka/goflow/utils/jsonx"
	"github.com/nyaruka/goflow/utils/uuids"
	"github.com/nyaruka/mailroom/config"
	"github.com/nyaruka/mailroom/testsuite"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type ServerTestCase struct {
	URL          string
	Method       string
	RequestFile  string
	Status       int
	ResponseFile string
	Response     string
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

			if !test.UpdateSnapshots {
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

// RunWebTests runs the tests in the passed in filename, optionally updating them if the update flag is set
func RunWebTests(t *testing.T, truthFile string) {
	rp := testsuite.RP()
	db := testsuite.DB()
	wg := &sync.WaitGroup{}

	uuids.SetGenerator(uuids.NewSeededGenerator(0))
	defer uuids.SetGenerator(uuids.DefaultGenerator)

	dates.SetNowSource(dates.NewSequentialNowSource(time.Date(2018, 7, 6, 12, 30, 0, 123456789, time.UTC)))
	defer dates.SetNowSource(dates.DefaultNowSource)

	server := NewServer(context.Background(), config.Mailroom, db, rp, nil, nil, wg)
	server.Start()
	defer server.Stop()

	// give our server time to start
	time.Sleep(time.Second)

	type TestCase struct {
		Label        string          `json:"label"`
		Method       string          `json:"method"`
		Path         string          `json:"path"`
		Body         json.RawMessage `json:"body"`
		Status       int             `json:"status"`
		Response     json.RawMessage `json:"response,omitempty"`
		ResponseFile string          `json:"response_file,omitempty"`
		DBAssertions []struct {
			Query string `json:"query"`
			Count int    `json:"count"`
		} `json:"db_assertions,omitempty"`

		actualResponse []byte
	}
	tcs := make([]*TestCase, 0, 20)
	tcJSON, err := ioutil.ReadFile(truthFile)
	require.NoError(t, err)

	err = json.Unmarshal(tcJSON, &tcs)
	require.NoError(t, err)

	for _, tc := range tcs {
		req, err := http.NewRequest(tc.Method, "http://localhost:8090"+tc.Path, bytes.NewReader([]byte(tc.Body)))
		assert.NoError(t, err, "%s: error creating request", tc.Label)

		resp, err := http.DefaultClient.Do(req)
		assert.NoError(t, err, "%s: error making request", tc.Label)

		assert.Equal(t, tc.Status, resp.StatusCode, "%s: unexpected status", tc.Label)

		tc.actualResponse, err = ioutil.ReadAll(resp.Body)
		assert.NoError(t, err, "%s: error reading body", tc.Label)

		var expectedResponse []byte

		if tc.ResponseFile != "" {
			expectedResponse, err = ioutil.ReadFile(tc.ResponseFile)
			require.NoError(t, err)
		}

		if !test.UpdateSnapshots {
			test.AssertEqualJSON(t, expectedResponse, tc.actualResponse, "%s: unexpected response", tc.Label)
		}

		for _, dba := range tc.DBAssertions {
			testsuite.AssertQueryCount(t, db, dba.Query, nil, dba.Count, "%s: '%s' returned wrong count", tc.Label, dba.Query)
		}
	}

	// update if we are meant to
	if test.UpdateSnapshots {
		truth, err := jsonx.MarshalPretty(tcs)
		require.NoError(t, err)

		err = ioutil.WriteFile(truthFile, truth, 0644)
		require.NoError(t, err, "failed to update truth file")

		for _, tc := range tcs {
			if tc.ResponseFile != "" {
				err = ioutil.WriteFile(tc.ResponseFile, tc.actualResponse, 0644)
				require.NoError(t, err, "failed to update response file")
			}
		}
	}
}
