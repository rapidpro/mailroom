package web

import (
	"bytes"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nyaruka/mailroom/config"
	"github.com/nyaruka/mailroom/testsuite"

	"github.com/stretchr/testify/assert"
)

func TestServer(t *testing.T) {
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

	tcs := []struct {
		URL      string
		Method   string
		Body     string
		Status   int
		Response string
	}{
		{"/arst", "GET", "", 404, "not found"},
		{"/", "POST", "", 405, "illegal"},
		{"/", "GET", "", 200, "mailroom"},
		{"/mr/", "POST", "", 405, "illegal"},
		{"/mr/", "GET", "", 200, "mailroom"},
	}

	for i, tc := range tcs {
		var body io.Reader

		if tc.Body != "" {
			body = bytes.NewReader([]byte(tc.Body))
		}

		req, err := http.NewRequest(tc.Method, "http://localhost:8090"+tc.URL, body)
		assert.NoError(t, err, "%d: error creating request", i)

		resp, err := http.DefaultClient.Do(req)
		assert.NoError(t, err, "%d: error making request", i)

		assert.Equal(t, tc.Status, resp.StatusCode, "%d: unexpected status", i)

		content, err := ioutil.ReadAll(resp.Body)
		assert.NoError(t, err, "%d: error reading body", i)

		assert.True(t, strings.Contains(string(content), tc.Response), "%d: did not find string: %s in body: %s", i, tc.Response, string(content))
	}
}
