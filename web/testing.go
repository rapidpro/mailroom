package web

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"strings"
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

// RunWebTests runs the tests in the passed in filename, optionally updating them if the update flag is set
func RunWebTests(t *testing.T, truthFile string) {
	rp := testsuite.RP()
	db := testsuite.DB()
	wg := &sync.WaitGroup{}

	defer uuids.SetGenerator(uuids.DefaultGenerator)
	defer dates.SetNowSource(dates.DefaultNowSource)

	server := NewServer(context.Background(), config.Mailroom, db, rp, nil, nil, wg)
	server.Start()
	defer server.Stop()

	// give our server time to start
	time.Sleep(time.Second)

	type TestCase struct {
		Label        string            `json:"label"`
		Method       string            `json:"method"`
		Path         string            `json:"path"`
		Body         json.RawMessage   `json:"body,omitempty"`
		Files        map[string]string `json:"files,omitempty"`
		Status       int               `json:"status"`
		Response     json.RawMessage   `json:"response,omitempty"`
		ResponseFile string            `json:"response_file,omitempty"`
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
		uuids.SetGenerator(uuids.NewSeededGenerator(123456))
		dates.SetNowSource(dates.NewSequentialNowSource(time.Date(2018, 7, 6, 12, 30, 0, 123456789, time.UTC)))

		var req *http.Request
		if len(tc.Files) > 0 {
			values := make(map[string][]string)
			err = json.Unmarshal(tc.Body, &values)
			require.NoError(t, err)

			req, err = makeMultipartRequest(tc.Method, "http://localhost:8090"+tc.Path, values, tc.Files)
		} else {
			req, err = http.NewRequest(tc.Method, "http://localhost:8090"+tc.Path, bytes.NewReader([]byte(tc.Body)))
		}
		assert.NoError(t, err, "%s: error creating request", tc.Label)

		resp, err := http.DefaultClient.Do(req)
		assert.NoError(t, err, "%s: error making request", tc.Label)

		assert.Equal(t, tc.Status, resp.StatusCode, "%s: unexpected status", tc.Label)

		tc.actualResponse, err = ioutil.ReadAll(resp.Body)
		assert.NoError(t, err, "%s: error reading body", tc.Label)

		if !test.UpdateSnapshots {
			var expectedResponse []byte
			expectedIsJSON := false

			if tc.ResponseFile != "" {
				expectedResponse, err = ioutil.ReadFile(tc.ResponseFile)
				require.NoError(t, err)

				expectedIsJSON = strings.HasSuffix(tc.ResponseFile, ".json")
			} else {
				expectedResponse = tc.Response
				expectedIsJSON = true
			}

			if expectedIsJSON {
				test.AssertEqualJSON(t, expectedResponse, tc.actualResponse, "%s: unexpected JSON response", tc.Label)
			} else {
				assert.Equal(t, string(expectedResponse), string(tc.actualResponse), "%s: unexpected response", tc.Label)
			}
		}

		for _, dba := range tc.DBAssertions {
			testsuite.AssertQueryCount(t, db, dba.Query, nil, dba.Count, "%s: '%s' returned wrong count", tc.Label, dba.Query)
		}
	}

	// update if we are meant to
	if test.UpdateSnapshots {
		for _, tc := range tcs {
			if tc.ResponseFile != "" {
				err = ioutil.WriteFile(tc.ResponseFile, tc.actualResponse, 0644)
				require.NoError(t, err, "failed to update response file")
			} else {
				tc.Response = tc.actualResponse
			}
		}

		truth, err := jsonx.MarshalPretty(tcs)
		require.NoError(t, err)

		err = ioutil.WriteFile(truthFile, truth, 0644)
		require.NoError(t, err, "failed to update truth file")
	}
}

func makeMultipartRequest(method, url string, fields map[string][]string, files map[string]string) (*http.Request, error) {
	b := &bytes.Buffer{}
	w := multipart.NewWriter(b)

	for key, values := range fields {
		for _, value := range values {
			fw, err := w.CreateFormField(key)
			if err != nil {
				return nil, err
			}
			io.WriteString(fw, value)
		}
	}
	for key, value := range files {
		fw, err := w.CreateFormFile(key, key)
		if err != nil {
			return nil, err
		}
		io.WriteString(fw, value)
	}

	w.Close()

	req, _ := http.NewRequest(method, url, bytes.NewReader(b.Bytes()))
	req.Header.Set("Content-Type", w.FormDataContentType())
	return req, nil
}
