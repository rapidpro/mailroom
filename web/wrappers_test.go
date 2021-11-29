package web_test

import (
	"context"
	"fmt"
	"github.com/jmoiron/sqlx"
	"net/http"
	"testing"
	"time"

	"github.com/greatnonprofits-nfp/goflow/flows"
	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/mailroom/core/models"
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

func TestRequireUserToken(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()
	testsuite.ResetDB()

	handler := getMockHandler()
	userId := int64(2)
	requestHandler := web.RequireUserToken(handler)
	server := &web.Server{DB: db, CTX: ctx}

	req1, err := http.NewRequest("GET", "https://temba.io", nil)
	assert.NoError(t, err)

	req2, err := http.NewRequest("GET", "https://example.com", nil)
	assert.NoError(t, err)

	response, status, _ := requestHandler(ctx, server, req1)
	_, ok := response.(error)
	assert.True(t, ok)
	assert.Equal(t, 401, status)

	response, status, _ = requestHandler(ctx, server, req2)
	_, ok = response.(error)
	assert.True(t, ok)
	assert.Equal(t, 401, status)

	addUserToken(db, userId)
	req2.Header.Set("authorization", "Token some-token")
	assert.NoError(t, err)
	response, status, _ = requestHandler(ctx, server, req2)
	assert.Equal(t, 200, status)
	assert.Equal(t, userId, response)
}

func getMockHandler() web.JSONHandler {
	return func(ctx context.Context, s *web.Server, r *http.Request) (interface{}, int, error) {
		fmt.Println(ctx.Value("user_id"))
		return ctx.Value("user_id"), 200, nil
	}
}

func addUserToken(db *sqlx.DB, userId int64) {
	db.MustExec("INSERT INTO api_apitoken(key, org_id, role_id, user_id, is_active, created) VALUES ($1, $2, $3, $4, TRUE, $5)",
		"some-token", 1, 8, userId, time.Now())
}
