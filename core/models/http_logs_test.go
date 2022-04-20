package models_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHTTPLogs(t *testing.T) {
	ctx, _, db, _ := testsuite.Get()

	defer func() { db.MustExec(`DELETE FROM request_logs_httplog`) }()

	// insert a classifier log
	log := models.NewClassifierCalledLog(testdata.Org1.ID, testdata.Wit.ID, "http://foo.bar", 200, "GET /", "STATUS 200", false, time.Second, 0, time.Now())
	err := models.InsertHTTPLogs(ctx, db, []*models.HTTPLog{log})
	assert.Nil(t, err)

	assertdb.Query(t, db, `SELECT count(*) from request_logs_httplog WHERE org_id = $1 AND status_code = 200 AND classifier_id = $2 AND is_error = FALSE`, testdata.Org1.ID, testdata.Wit.ID).Returns(1)

	// insert a log with nil response
	log = models.NewClassifierCalledLog(testdata.Org1.ID, testdata.Wit.ID, "http://foo.bar", 0, "GET /", "", true, time.Second, 0, time.Now())
	err = models.InsertHTTPLogs(ctx, db, []*models.HTTPLog{log})
	assert.Nil(t, err)

	assertdb.Query(t, db, `SELECT count(*) from request_logs_httplog WHERE org_id = $1 AND status_code = 0 AND classifier_id = $2 AND is_error = TRUE AND response IS NULL`, testdata.Org1.ID, testdata.Wit.ID).Returns(1)

	// insert a webhook log
	log = models.NewWebhookCalledLog(testdata.Org1.ID, testdata.Favorites.ID, "http://foo.bar", 400, "GET /", "HTTP 200", false, time.Second, 2, time.Now())
	err = models.InsertHTTPLogs(ctx, db, []*models.HTTPLog{log})
	assert.Nil(t, err)

	assertdb.Query(t, db, `SELECT count(*) from request_logs_httplog WHERE org_id = $1 AND status_code = 400 AND flow_id = $2 AND num_retries = 2`, testdata.Org1.ID, testdata.Favorites.ID).Returns(1)
}

func TestHTTPLogger(t *testing.T) {
	ctx, _, db, _ := testsuite.Get()

	defer func() { db.MustExec(`DELETE FROM request_logs_httplog`) }()

	defer httpx.SetRequestor(httpx.DefaultRequestor)
	httpx.SetRequestor(httpx.NewMockRequestor(map[string][]httpx.MockResponse{
		"https://temba.io": {
			httpx.NewMockResponse(200, nil, `hello`),
			httpx.NewMockResponse(400, nil, `world`),
		},
	}))

	mailgun, err := models.LookupTicketerByUUID(ctx, db, testdata.Mailgun.UUID)
	require.NoError(t, err)

	logger := &models.HTTPLogger{}
	log := logger.Ticketer(mailgun)

	// make and log a few HTTP requests
	req1, err := http.NewRequest("GET", "https://temba.io", nil)
	require.NoError(t, err)
	trace1, err := httpx.DoTrace(http.DefaultClient, req1, nil, nil, -1)
	require.NoError(t, err)
	log(flows.NewHTTPLog(trace1, flows.HTTPStatusFromCode, nil))

	req2, err := http.NewRequest("GET", "https://temba.io", nil)
	require.NoError(t, err)
	trace2, err := httpx.DoTrace(http.DefaultClient, req2, nil, nil, -1)
	require.NoError(t, err)
	log(flows.NewHTTPLog(trace2, flows.HTTPStatusFromCode, nil))

	err = logger.Insert(ctx, db)
	assert.NoError(t, err)

	assertdb.Query(t, db, `SELECT count(*) from request_logs_httplog WHERE org_id = $1 AND ticketer_id = $2`, testdata.Org1.ID, testdata.Mailgun.ID).Returns(2)
}
