package models_test

import (
	"net/http"
	"testing"

	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/testsuite"

	"github.com/stretchr/testify/require"
)

func TestChannelLogs(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()

	db.MustExec(`DELETE FROM channels_channellog;`)

	defer httpx.SetRequestor(httpx.DefaultRequestor)
	httpx.SetRequestor(httpx.NewMockRequestor(map[string][]httpx.MockResponse{
		"http://rapidpro.io":     {httpx.NewMockResponse(200, nil, "OK")},
		"http://rapidpro.io/bad": {httpx.NewMockResponse(400, nil, "Oops")},
	}))

	oa, err := models.GetOrgAssets(ctx, db, models.Org1)
	require.NoError(t, err)

	channel := oa.ChannelByID(models.TwilioChannelID)

	req1, _ := httpx.NewRequest("GET", "http://rapidpro.io", nil, nil)
	trace1, err := httpx.DoTrace(http.DefaultClient, req1, nil, nil, -1)
	log1 := models.NewChannelLog(trace1, false, "test request", channel, nil)

	req2, _ := httpx.NewRequest("GET", "http://rapidpro.io/bad", nil, nil)
	trace2, err := httpx.DoTrace(http.DefaultClient, req2, nil, nil, -1)
	log2 := models.NewChannelLog(trace2, true, "test request", channel, nil)

	err = models.InsertChannelLogs(ctx, db, []*models.ChannelLog{log1, log2})
	require.NoError(t, err)

	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM channels_channellog`, nil, 2)
	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM channels_channellog WHERE url = 'http://rapidpro.io' AND is_error = FALSE AND channel_id = $1`, []interface{}{channel.ID()}, 1)
	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM channels_channellog WHERE url = 'http://rapidpro.io/bad' AND is_error = TRUE AND channel_id = $1`, []interface{}{channel.ID()}, 1)
}
