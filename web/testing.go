package web

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/test"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/testsuite"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// RunWebTests runs the tests in the passed in filename, optionally updating them if the update flag is set
func RunWebTests(t *testing.T, ctx context.Context, rt *runtime.Runtime, truthFile string, substitutions map[string]string) {
	wg := &sync.WaitGroup{}

	defer uuids.SetGenerator(uuids.DefaultGenerator)
	uuids.SetGenerator(uuids.NewSeededGenerator(123456))

	defer dates.SetNowSource(dates.DefaultNowSource)

	server := NewServer(ctx, rt, wg)
	server.Start()
	defer server.Stop()

	// give our server time to start
	time.Sleep(time.Second)

	type TestCase struct {
		Label        string               `json:"label"`
		HTTPMocks    *httpx.MockRequestor `json:"http_mocks,omitempty"`
		Method       string               `json:"method"`
		Path         string               `json:"path"`
		Headers      map[string]string    `json:"headers,omitempty"`
		Body         json.RawMessage      `json:"body,omitempty"`
		BodyEncode   string               `json:"body_encode,omitempty"`
		Status       int                  `json:"status"`
		Response     json.RawMessage      `json:"response,omitempty"`
		ResponseFile string               `json:"response_file,omitempty"`
		DBAssertions []struct {
			Query string `json:"query"`
			Count int    `json:"count"`
		} `json:"db_assertions,omitempty"`

		actualResponse []byte
	}
	tcs := make([]TestCase, 0, 20)
	tcJSON, err := os.ReadFile(truthFile)
	require.NoError(t, err)

	for key, value := range substitutions {
		tcJSON = bytes.ReplaceAll(tcJSON, []byte("$"+key+"$"), []byte(value))
	}

	jsonx.MustUnmarshal(tcJSON, &tcs)

	for i, tc := range tcs {
		dates.SetNowSource(dates.NewSequentialNowSource(time.Date(2018, 7, 6, 12, 30, 0, 123456789, time.UTC)))

		var clonedMocks *httpx.MockRequestor
		if tc.HTTPMocks != nil {
			httpx.SetRequestor(tc.HTTPMocks)
			clonedMocks = tc.HTTPMocks.Clone()
		} else {
			httpx.SetRequestor(httpx.DefaultRequestor)
		}

		testURL := "http://localhost:8090" + tc.Path
		var req *http.Request
		if tc.BodyEncode == "multipart" {
			var parts []MultiPartPart
			jsonx.MustUnmarshal(tc.Body, &parts)

			req, err = MakeMultipartRequest(tc.Method, testURL, parts, tc.Headers)

		} else if len(tc.Body) >= 2 && tc.Body[0] == '"' { // if body is a string, treat it as a URL encoded submission
			bodyStr := ""
			jsonx.MustUnmarshal(tc.Body, &bodyStr)
			bodyReader := strings.NewReader(bodyStr)
			req, err = httpx.NewRequest(tc.Method, testURL, bodyReader, tc.Headers)
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		} else {
			bodyReader := bytes.NewReader([]byte(tc.Body))
			req, err = httpx.NewRequest(tc.Method, testURL, bodyReader, tc.Headers)
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		}

		assert.NoError(t, err, "%s: error creating request", tc.Label)

		resp, err := http.DefaultClient.Do(req)
		assert.NoError(t, err, "%s: error making request", tc.Label)

		// check all http mocks were used
		if tc.HTTPMocks != nil {
			assert.False(t, tc.HTTPMocks.HasUnused(), "%s: unused HTTP mocks in %s", tc.Label)
		}

		// clone test case and populate with actual values
		actual := tc
		actual.Status = resp.StatusCode
		actual.HTTPMocks = clonedMocks

		tc.HTTPMocks = clonedMocks
		tc.actualResponse, err = io.ReadAll(resp.Body)
		assert.NoError(t, err, "%s: error reading body", tc.Label)

		if !test.UpdateSnapshots {
			assert.Equal(t, tc.Status, actual.Status, "%s: unexpected status", tc.Label)

			var expectedResponse []byte
			expectedIsJSON := false

			if tc.ResponseFile != "" {
				expectedResponse, err = os.ReadFile(tc.ResponseFile)
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

			for _, dba := range tc.DBAssertions {
				testsuite.AssertQuery(t, rt.DB, dba.Query).Returns(dba.Count, "%s: '%s' returned wrong count", tc.Label, dba.Query)
			}

		} else {
			tcs[i] = actual
		}
	}

	// update if we are meant to
	if test.UpdateSnapshots {
		for _, tc := range tcs {
			if tc.ResponseFile != "" {
				err = os.WriteFile(tc.ResponseFile, tc.actualResponse, 0644)
				require.NoError(t, err, "failed to update response file")
			} else {
				tc.Response = tc.actualResponse
			}
		}

		truth, err := jsonx.MarshalPretty(tcs)
		require.NoError(t, err)

		err = os.WriteFile(truthFile, truth, 0644)
		require.NoError(t, err, "failed to update truth file")
	}
}

// MultiPartPart is a single part in a multipart encoded request
type MultiPartPart struct {
	Name        string `json:"name"`
	Filename    string `json:"filename"`
	ContentType string `json:"content-type"`
	Data        string `json:"data"`
}

// MakeMultipartRequest makes a multipart encoded request
func MakeMultipartRequest(method, url string, parts []MultiPartPart, headers map[string]string) (*http.Request, error) {
	b := &bytes.Buffer{}
	w := multipart.NewWriter(b)

	for _, part := range parts {
		var fw io.Writer
		var err error
		if part.Filename != "" {
			contentType := part.ContentType
			if contentType == "" {
				contentType = "application/octet-stream"
			}

			h := make(textproto.MIMEHeader)
			h.Set("Content-Disposition", fmt.Sprintf(`form-data; name="%s"; filename="%s"`, part.Name, part.Filename))
			h.Set("Content-Type", contentType)
			fw, err = w.CreatePart(h)
		} else {
			fw, err = w.CreateFormField(part.Name)
		}
		if err != nil {
			return nil, err
		}
		io.WriteString(fw, part.Data)
	}

	w.Close()

	req, _ := httpx.NewRequest(method, url, bytes.NewReader(b.Bytes()), headers)
	req.Header.Set("Content-Type", w.FormDataContentType())
	return req, nil
}
