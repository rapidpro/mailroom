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

	"github.com/nyaruka/mailroom/config"
	"github.com/nyaruka/mailroom/models"
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

	// add a trigger for our campaign flow with 'trigger'
	db.MustExec(
		`INSERT INTO triggers_trigger(is_active, created_on, modified_on, keyword, is_archived, 
									  flow_id, trigger_type, match_type, created_by_id, modified_by_id, org_id, trigger_count)
		VALUES(TRUE, now(), now(), 'trigger', false, $1, 'K', 'O', 1, 1, 1, 0) RETURNING id`,
		models.CampaignFlowID,
	)

	// also add a catch all
	db.MustExec(
		`INSERT INTO triggers_trigger(is_active, created_on, modified_on, keyword, is_archived, 
									  flow_id, trigger_type, match_type, created_by_id, modified_by_id, org_id, trigger_count)
		VALUES(TRUE, now(), now(), NULL, false, $1, 'C', NULL, 1, 1, 1, 0) RETURNING id`,
		models.CampaignFlowID,
	)

	tcs := []struct {
		URL      string
		Method   string
		BodyFile string
		Status   int
		Response string
	}{
		{"/mr/flow/migrate", "GET", "", 405, "illegal"},
		{"/mr/flow/migrate", "POST", "testdata/migrate_minimal_legacy.json", 200, `"type": "send_msg"`},
		{"/mr/flow/validate", "GET", "", 405, "illegal"},
		{"/mr/flow/validate", "POST", "testdata/validate_valid_legacy.json", 200, `"type": "send_msg"`},
		{"/mr/flow/validate", "POST", "testdata/validate_invalid_legacy.json", 422, `"error": "missing dependencies:`},
		{"/mr/flow/validate", "POST", "testdata/validate_valid.json", 200, `"type": "send_msg"`},
		{"/mr/flow/validate", "POST", "testdata/validate_invalid.json", 422, `isn't a known node`},
		{"/mr/flow/validate", "POST", "testdata/validate_valid_without_assets.json", 200, `"type": "send_msg"`},
		{"/mr/flow/validate", "POST", "testdata/validate_legacy_single_msg.json", 200, `"type": "send_msg"`},
		{"/mr/flow/clone", "POST", "testdata/clone_valid.json", 200, `"type": "send_msg"`},
	}

	for _, tc := range tcs {
		testID := fmt.Sprintf("%s %s %s", tc.Method, tc.URL, tc.BodyFile)
		var body io.Reader
		var err error

		if tc.BodyFile != "" {
			body, err = os.Open(tc.BodyFile)
			require.NoError(t, err, "unable to open %s", tc.BodyFile)
		}

		req, err := http.NewRequest(tc.Method, "http://localhost:8090"+tc.URL, body)
		assert.NoError(t, err, "error creating request in %s", testID)

		resp, err := http.DefaultClient.Do(req)
		assert.NoError(t, err, "error making request in %s", testID)

		content, err := ioutil.ReadAll(resp.Body)
		require.NoError(t, err, "error reading body in %s", testID)

		assert.Equal(t, tc.Status, resp.StatusCode, "unexpected status in %s (response=%s)", testID, content)
		assert.Contains(t, string(content), tc.Response, "response mismatch in ", testID)
	}
}
