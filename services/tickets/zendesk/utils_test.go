package zendesk_test

import (
	"testing"
	"time"

	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/mailroom/services/tickets/zendesk"

	"github.com/stretchr/testify/assert"
)

func TestRequestID(t *testing.T) {
	defer dates.SetNowSource(dates.DefaultNowSource)
	dates.SetNowSource(dates.NewSequentialNowSource(time.Date(2019, 10, 7, 15, 21, 30, 123456789, time.UTC)))

	id1 := zendesk.NewRequestID("sesame")

	assert.Equal(t, "sesame:1570461690123456789", id1.String())

	id2, err := zendesk.ParseRequestID("sesame:1570461690123456789")
	assert.NoError(t, err)
	assert.Equal(t, "sesame", id2.Secret)
	assert.True(t, id2.Timestamp.Equal(time.Date(2019, 10, 7, 15, 21, 30, 123456789, time.UTC)))

	_, err = zendesk.ParseRequestID("sesame")
	assert.EqualError(t, err, "invalid request ID format")

	_, err = zendesk.ParseRequestID("sesame:abc")
	assert.EqualError(t, err, "invalid request ID format")
}
