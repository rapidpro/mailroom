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

	"github.com/greatnonprofits-nfp/goflow/test"
	"github.com/greatnonprofits-nfp/goflow/utils/dates"
	"github.com/greatnonprofits-nfp/goflow/utils/uuids"
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
		Response     json.RawMessage `json:"response"`
		DBAssertions []struct {
			Query string `json:"query"`
			Count int    `json:"count"`
		} `json:"db_assertions,omitempty"`
	}
	tcs := make([]*TestCase, 0, 20)
	tcJSON, err := ioutil.ReadFile(truthFile)
	assert.NoError(t, err)

	err = json.Unmarshal(tcJSON, &tcs)
	assert.NoError(t, err)

	for _, tc := range tcs {
		req, err := http.NewRequest(tc.Method, "http://localhost:8090"+tc.Path, bytes.NewReader([]byte(tc.Body)))
		assert.NoError(t, err, "%s: error creating request", tc.Label)

		resp, err := http.DefaultClient.Do(req)
		assert.NoError(t, err, "%s: error making request", tc.Label)

		assert.Equal(t, tc.Status, resp.StatusCode, "%s: unexpected status", tc.Label)

		response, err := ioutil.ReadAll(resp.Body)
		assert.NoError(t, err, "%s: error reading body", tc.Label)

		if !test.UpdateSnapshots {
			test.AssertEqualJSON(t, json.RawMessage(tc.Response), json.RawMessage(response), "%s: unexpected response\nExpected:\n%s\nGot:\n%s", tc.Label, tc.Response, string(response))
		}

		for _, dba := range tc.DBAssertions {
			testsuite.AssertQueryCount(t, db, dba.Query, nil, dba.Count, "%s: '%s' returned wrong count", tc.Label, dba.Query)
		}

		tc.Response = json.RawMessage(response)
	}

	// update if we are meant to
	if test.UpdateSnapshots {
		truth, err := json.MarshalIndent(tcs, "", "    ")
		assert.NoError(t, err)

		if err := ioutil.WriteFile(truthFile, truth, 0644); err != nil {
			require.NoError(t, err, "failed to update truth file")
		}
	}
}
