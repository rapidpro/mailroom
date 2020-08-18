package web_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/web"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWithHTTPLogs(t *testing.T) {
	testsuite.ResetDB()

	defer httpx.SetRequestor(httpx.DefaultRequestor)
	httpx.SetRequestor(httpx.NewMockRequestor(map[string][]httpx.MockResponse{
		"https://temba.io": {
			httpx.NewMockResponse(200, nil, `hello`),
			httpx.NewMockResponse(400, nil, `world`),
		},
	}))

	handler := func(ctx context.Context, s *web.Server, r *http.Request, l *models.HTTPLogger) (interface{}, int, error) {
		ticketer, _ := models.LookupTicketerByUUID(ctx, s.DB, models.MailgunUUID)

		logger := l.Ticketer(ticketer)

		// make and log a few HTTP requests
		req1, err := http.NewRequest("GET", "https://temba.io", nil)
		require.NoError(t, err)
		trace1, err := httpx.DoTrace(http.DefaultClient, req1, nil, nil, -1)
		require.NoError(t, err)
		logger(flows.NewHTTPLog(trace1, flows.HTTPStatusFromCode, nil))

		req2, err := http.NewRequest("GET", "https://temba.io", nil)
		require.NoError(t, err)
		trace2, err := httpx.DoTrace(http.DefaultClient, req2, nil, nil, -1)
		require.NoError(t, err)
		logger(flows.NewHTTPLog(trace2, flows.HTTPStatusFromCode, nil))

		return map[string]string{"status": "OK"}, http.StatusOK, nil
	}

	// simulate handler being invoked by server
	server := &web.Server{DB: testsuite.DB()}
	wrapped := web.WithHTTPLogs(handler)
	response, status, err := wrapped(testsuite.CTX(), server, nil)

	// check response from handler
	assert.Equal(t, map[string]string{"status": "OK"}, response)
	assert.Equal(t, http.StatusOK, status)
	assert.NoError(t, err)

	// check HTTP logs were created
	testsuite.AssertQueryCount(t, testsuite.DB(), `select count(*) from request_logs_httplog where ticketer_id = $1;`, []interface{}{models.MailgunID}, 2)
}
