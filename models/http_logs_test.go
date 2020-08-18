package models_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/testsuite"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHTTPLogs(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()

	// insert a log
	log := models.NewClassifierCalledLog(models.Org1, models.WitID, "http://foo.bar", "GET /", "STATUS 200", false, time.Second, time.Now())
	err := models.InsertHTTPLogs(ctx, db, []*models.HTTPLog{log})
	assert.Nil(t, err)

	testsuite.AssertQueryCount(t, db,
		`SELECT count(*) from request_logs_httplog WHERE org_id = $1 AND classifier_id = $2 AND is_error = FALSE`,
		[]interface{}{models.Org1, models.WitID}, 1)

	// insert a log with nil response
	log = models.NewClassifierCalledLog(models.Org1, models.WitID, "http://foo.bar", "GET /", "", true, time.Second, time.Now())
	err = models.InsertHTTPLogs(ctx, db, []*models.HTTPLog{log})
	assert.Nil(t, err)

	testsuite.AssertQueryCount(t, db,
		`SELECT count(*) from request_logs_httplog WHERE org_id = $1 AND classifier_id = $2 AND is_error = TRUE AND response IS NULL`,
		[]interface{}{models.Org1, models.WitID}, 1)
}

func TestHTTPLogger(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()

	defer httpx.SetRequestor(httpx.DefaultRequestor)
	httpx.SetRequestor(httpx.NewMockRequestor(map[string][]httpx.MockResponse{
		"https://temba.io": {
			httpx.NewMockResponse(200, nil, `hello`),
			httpx.NewMockResponse(400, nil, `world`),
		},
	}))

	mailgun, err := models.LookupTicketerByUUID(ctx, db, models.MailgunUUID)
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

	testsuite.AssertQueryCount(t, db,
		`SELECT count(*) from request_logs_httplog WHERE org_id = $1 AND ticketer_id = $2`,
		[]interface{}{models.Org1, models.MailgunID}, 2)
}
