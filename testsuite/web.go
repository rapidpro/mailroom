package testsuite

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
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nyaruka/gocommon/aws/dynamo"
	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/goflow/test"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/web"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// RunWebTests runs the tests in the passed in filename, optionally updating them if the update flag is set
func RunWebTests(t *testing.T, rt *runtime.Runtime, truthFile string) {
	ctx := t.Context()

	wg := &sync.WaitGroup{}
	server := web.NewServer(ctx, rt, wg)
	server.Start()
	defer server.Stop()

	time.Sleep(100 * time.Millisecond) // give server time to start

	type TestCase struct {
		Label           string                `json:"label"`
		HTTPMocks       *httpx.MockRequestor  `json:"http_mocks,omitempty"`
		Method          string                `json:"method"`
		Path            string                `json:"path"`
		Headers         map[string]string     `json:"headers,omitempty"`
		Body            json.RawMessage       `json:"body,omitempty"`
		BodyEncode      string                `json:"body_encode,omitempty"`
		Status          int                   `json:"status"`
		Response        json.RawMessage       `json:"response,omitempty"`
		ResponseFile    string                `json:"response_file,omitempty"`
		DBAssertions    []*assertdb.Assert    `json:"db_assertions,omitempty"`
		ExpectedTasks   map[string][]TaskInfo `json:"expected_tasks,omitempty"`
		ExpectedHistory []*dynamo.Item        `json:"expected_history,omitempty"`
		IndexedMessages []IndexedMessage      `json:"indexed_messages,omitempty"`

		actualResponse  []byte
		expectsJSONBody bool
	}
	tcs := make([]TestCase, 0, 20)
	tcJSON := ReadFile(t, truthFile)

	jsonx.MustUnmarshal(tcJSON, &tcs)
	var err error

	test.MockUniverse()

	// track which messages were already indexed before each test case so we only report new ones
	prevIndexedIDs := make(map[string]bool)
	for _, m := range GetIndexedMessages(t, rt, false) {
		prevIndexedIDs[m.ID] = true
	}

	for i, tc := range tcs {
		var clonedMocks *httpx.MockRequestor
		if tc.HTTPMocks != nil {
			tc.HTTPMocks.SetIgnoreLocal(true)
			httpx.SetRequestor(tc.HTTPMocks)
			clonedMocks = tc.HTTPMocks.Clone()
		} else {
			httpx.SetRequestor(httpx.DefaultRequestor)
		}

		testURL := "http://localhost:8091" + tc.Path
		var req *http.Request
		if tc.BodyEncode == "multipart" {
			var parts []MultiPartPart
			jsonx.MustUnmarshal(tc.Body, &parts)

			req, err = MakeMultipartRequest(tc.Method, testURL, parts, tc.Headers)

		} else if len(tc.Body) >= 2 && tc.Body[0] == '"' { // if body is a string, treat it as a URL encoded submission
			bodyStr := ""
			jsonx.MustUnmarshal(tc.Body, &bodyStr)
			bodyReader := strings.NewReader(bodyStr)
			req, err = httpx.NewRequest(ctx, tc.Method, testURL, bodyReader, tc.Headers)
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		} else {
			bodyReader := bytes.NewReader([]byte(tc.Body))
			req, err = httpx.NewRequest(ctx, tc.Method, testURL, bodyReader, tc.Headers)
			req.Header.Set("Content-Type", "application/json")
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
		actual.actualResponse, err = io.ReadAll(resp.Body)
		actual.ExpectedTasks = GetQueuedTasks(t, rt)
		actual.ExpectedHistory = GetHistoryItems(t, rt, true, test.MockStartTime)
		allIndexed := GetIndexedMessages(t, rt, false)
		actual.IndexedMessages = make([]IndexedMessage, 0, len(allIndexed))
		for _, m := range allIndexed {
			if !prevIndexedIDs[m.ID] {
				actual.IndexedMessages = append(actual.IndexedMessages, m)
				prevIndexedIDs[m.ID] = true
			}
		}

		actual.DBAssertions = make([]*assertdb.Assert, len(tc.DBAssertions))
		for i, dba := range tc.DBAssertions {
			actual.DBAssertions[i] = dba.Actual(t, rt.DB)
		}

		assert.NoError(t, err, "%s: error reading body", tc.Label)

		// some timestamps come from db NOW() which we can't mock, so we replace them with $recent_timestamp$
		actual.actualResponse = overwriteRecentTimestamps(actual.actualResponse)

		ClearTasks(t, rt)

		if tc.ResponseFile != "" {
			actual.expectsJSONBody = strings.HasSuffix(tc.ResponseFile, ".json")
		} else if bytes.HasPrefix(tc.Response, []byte(`"`)) && bytes.HasSuffix(tc.Response, []byte(`"`)) {
			actual.expectsJSONBody = false
		} else {
			actual.expectsJSONBody = true
		}

		if !test.UpdateSnapshots {
			assert.Equal(t, tc.Status, actual.Status, "%s: unexpected status", tc.Label)

			var expectedResponse []byte

			if tc.ResponseFile != "" {
				expectedResponse = ReadFile(t, tc.ResponseFile)
			} else {
				expectedResponse = tc.Response

				// if response is a single string.. treat it as a text/plain response, otherwise as JSON
				if !actual.expectsJSONBody {
					var responseText string
					jsonx.MustUnmarshal(expectedResponse, &responseText)
					expectedResponse = []byte(responseText)
				}
			}

			if actual.expectsJSONBody {
				assert.Equal(t, "application/json", resp.Header.Get("Content-Type"), "%s: unexpected content type", tc.Label)

				test.AssertEqualJSON(t, expectedResponse, actual.actualResponse, "%s: unexpected JSON response", tc.Label)
			} else {
				assert.Equal(t, string(expectedResponse), string(actual.actualResponse), "%s: unexpected response", tc.Label)
			}

			for _, dba := range tc.DBAssertions {
				dba.Check(t, rt.DB, "%s: assertion for query '%s' failed", tc.Label, dba.Query)
			}

			if tc.ExpectedTasks == nil {
				tc.ExpectedTasks = map[string][]TaskInfo{}
			}
			test.AssertEqualJSON(t, jsonx.MustMarshal(tc.ExpectedTasks), jsonx.MustMarshal(actual.ExpectedTasks), "%s: unexpected tasks", tc.Label)

			if tc.ExpectedHistory == nil {
				tc.ExpectedHistory = []*dynamo.Item{}
			}
			test.AssertEqualJSON(t, jsonx.MustMarshal(tc.ExpectedHistory), jsonx.MustMarshal(actual.ExpectedHistory), "%s: event history mismatch", tc.Label)

			if tc.IndexedMessages == nil {
				tc.IndexedMessages = []IndexedMessage{}
			}
			if actual.IndexedMessages == nil {
				actual.IndexedMessages = []IndexedMessage{}
			}
			test.AssertEqualJSON(t, jsonx.MustMarshal(tc.IndexedMessages), jsonx.MustMarshal(actual.IndexedMessages), "%s: indexed messages mismatch", tc.Label)

		} else {
			tcs[i] = actual
		}
	}

	// update if we are meant to
	if test.UpdateSnapshots {
		for i := range tcs {
			if tcs[i].ResponseFile != "" {
				err = os.WriteFile(tcs[i].ResponseFile, tcs[i].actualResponse, 0644)
				require.NoError(t, err, "failed to update response file")
			} else if tcs[i].expectsJSONBody {
				tcs[i].Response = tcs[i].actualResponse
			} else {
				tcs[i].Response = jsonx.MustMarshal(string(tcs[i].actualResponse))
			}
		}

		truth, err := jsonx.MarshalPretty(tcs)
		require.NoError(t, err)

		err = os.WriteFile(truthFile, truth, 0644)
		require.NoError(t, err, "failed to update truth file")
	}
}

var isoTimestampRegex = regexp.MustCompile(`\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{1,9}Z`)

func overwriteRecentTimestamps(resp []byte) []byte {
	return isoTimestampRegex.ReplaceAllFunc(resp, func(b []byte) []byte {
		t, _ := time.Parse(time.RFC3339, string(b))
		if time.Since(t) < time.Second*10 {
			return []byte(`$recent_timestamp$`)
		}
		return b
	})
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

	req, _ := httpx.NewRequest(context.Background(), method, url, bytes.NewReader(b.Bytes()), headers)
	req.Header.Set("Content-Type", w.FormDataContentType())
	return req, nil
}
