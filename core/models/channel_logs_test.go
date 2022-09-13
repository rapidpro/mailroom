package models_test

import (
	"errors"
	"net/http"
	"testing"

	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"

	"github.com/stretchr/testify/require"
)

func TestChannelLogsOutgoing(t *testing.T) {
	ctx, rt, db, _ := testsuite.Get()

	defer db.MustExec(`DELETE FROM channels_channellog`)

	defer httpx.SetRequestor(httpx.DefaultRequestor)
	httpx.SetRequestor(httpx.NewMockRequestor(map[string][]*httpx.MockResponse{
		"http://ivr.com/start":  {httpx.NewMockResponse(200, nil, []byte("OK"))},
		"http://ivr.com/hangup": {httpx.NewMockResponse(400, nil, []byte("Oops"))},
	}))

	oa, err := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
	require.NoError(t, err)

	channel := oa.ChannelByID(testdata.TwilioChannel.ID)
	require.NotNil(t, channel)

	clog1 := models.NewChannelLog(models.ChannelLogTypeIVRStart, channel, []string{"sesame"})
	clog2 := models.NewChannelLog(models.ChannelLogTypeIVRHangup, channel, []string{"sesame"})

	req1, _ := httpx.NewRequest("GET", "http://ivr.com/start", nil, map[string]string{"Authorization": "Token sesame"})
	trace1, err := httpx.DoTrace(http.DefaultClient, req1, nil, nil, -1)
	require.NoError(t, err)

	clog1.HTTP(trace1)
	clog1.End()

	req2, _ := httpx.NewRequest("GET", "http://ivr.com/hangup", nil, nil)
	trace2, err := httpx.DoTrace(http.DefaultClient, req2, nil, nil, -1)
	require.NoError(t, err)

	clog2.HTTP(trace2)
	clog2.Error(errors.New("oops"))
	clog2.End()

	err = models.InsertChannelLogs(ctx, db, []*models.ChannelLog{clog1, clog2})
	require.NoError(t, err)

	assertdb.Query(t, db, `SELECT count(*) FROM channels_channellog`).Returns(2)
	assertdb.Query(t, db, `SELECT count(*) FROM channels_channellog WHERE log_type = 'ivr_start' AND http_logs -> 0 ->> 'url' = 'http://ivr.com/start' AND is_error = FALSE AND channel_id = $1`, channel.ID()).Returns(1)
	assertdb.Query(t, db, `SELECT count(*) FROM channels_channellog WHERE log_type = 'ivr_hangup' AND http_logs -> 0 ->> 'url' = 'http://ivr.com/hangup' AND is_error = TRUE AND channel_id = $1`, channel.ID()).Returns(1)
	assertdb.Query(t, db, `SELECT count(*) FROM channels_channellog WHERE http_logs::text LIKE '%sesame%'`).Returns(0)
}
