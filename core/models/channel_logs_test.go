package models_test

import (
	"net/http"
	"testing"

	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"

	"github.com/stretchr/testify/require"
)

func TestChannelLogs(t *testing.T) {
	ctx, rt, db, _ := testsuite.Get()

	defer db.MustExec(`DELETE FROM channels_channellog`)

	defer httpx.SetRequestor(httpx.DefaultRequestor)
	httpx.SetRequestor(httpx.NewMockRequestor(map[string][]httpx.MockResponse{
		"http://rapidpro.io":     {httpx.NewMockResponse(200, nil, "OK")},
		"http://rapidpro.io/bad": {httpx.NewMockResponse(400, nil, "Oops")},
		"http://rapidpro.io/new": {httpx.NewMockResponse(200, nil, "OK")},
	}))

	oa, err := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
	require.NoError(t, err)

	channel := oa.ChannelByID(testdata.TwilioChannel.ID)
	require.NotNil(t, channel)

	req1, _ := httpx.NewRequest("GET", "http://rapidpro.io", nil, nil)
	trace1, err := httpx.DoTrace(http.DefaultClient, req1, nil, nil, -1)
	require.NoError(t, err)
	log1 := models.NewChannelLog(trace1, false, "test request", channel, nil)

	req2, _ := httpx.NewRequest("GET", "http://rapidpro.io/bad", nil, nil)
	trace2, err := httpx.DoTrace(http.DefaultClient, req2, nil, nil, -1)
	require.NoError(t, err)
	log2 := models.NewChannelLog(trace2, true, "test request", channel, nil)

	req3, _ := httpx.NewRequest("GET", "http://rapidpro.io/new", nil, map[string]string{"X-Forwarded-Path": "/old"})
	trace3, err := httpx.DoTrace(http.DefaultClient, req3, nil, nil, -1)
	require.NoError(t, err)
	log3 := models.NewChannelLog(trace3, false, "test request", channel, nil)

	err = models.InsertChannelLogs(ctx, db, []*models.ChannelLog{log1, log2, log3})
	require.NoError(t, err)

	testsuite.AssertQuery(t, db, `SELECT count(*) FROM channels_channellog`).Returns(3)
	testsuite.AssertQuery(t, db, `SELECT count(*) FROM channels_channellog WHERE url = 'http://rapidpro.io' AND is_error = FALSE AND channel_id = $1`, channel.ID()).Returns(1)
	testsuite.AssertQuery(t, db, `SELECT count(*) FROM channels_channellog WHERE url = 'http://rapidpro.io/bad' AND is_error = TRUE AND channel_id = $1`, channel.ID()).Returns(1)
	testsuite.AssertQuery(t, db, `SELECT count(*) FROM channels_channellog WHERE url = 'https://rapidpro.io/old' AND is_error = FALSE AND channel_id = $1`, channel.ID()).Returns(1)
}
