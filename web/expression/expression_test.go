package expression

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
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
		URL      string
		Method   string
		Body     string
		Status   int
		Response string
	}{
		{URL: "/mr/expression/migrate", Method: "GET", Status: 405, Response: `{"error": "illegal method: GET"}`},
		{URL: "/mr/expression/migrate", Method: "POST", Body: `{"expression":"@contact.age"}`, Status: 200, Response: `{"migrated":"@fields.age"}`},
		{URL: "/mr/expression/migrate", Method: "POST", Body: `{"expression":"@(UPPER(contact.tel))"}`, Status: 200, Response: `{"migrated":"@(upper(format_urn(urns.tel)))"}`},
		{URL: "/mr/expression/migrate", Method: "POST", Body: `{"expression":"@(+)"}`, Status: 422, Response: `{"error":"unable to migrate expression: error evaluating @(+): syntax error at +"}`},
	}

	for _, tc := range tcs {
		testID := fmt.Sprintf("%s %s %s", tc.Method, tc.URL, tc.Body)

		req, err := http.NewRequest(tc.Method, "http://localhost:8090"+tc.URL, strings.NewReader(tc.Body))
		assert.NoError(t, err, "error creating request in %s", testID)

		resp, err := http.DefaultClient.Do(req)
		assert.NoError(t, err, "error making request in %s", testID)

		content, err := ioutil.ReadAll(resp.Body)
		require.NoError(t, err, "error reading body in %s", testID)

		assert.Equal(t, tc.Status, resp.StatusCode, "unexpected status in %s (response=%s)", testID, content)

		test.AssertEqualJSON(t, []byte(tc.Response), content, "response mismatch in %s", testID)
	}
}
