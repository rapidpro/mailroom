package models

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/nyaruka/mailroom/testsuite"
	"github.com/stretchr/testify/assert"
)

func TestChannelEvents(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()

	start := time.Now()

	// no extra
	e := NewChannelEvent(MOMissEventType, Org1, TwilioChannelID, CathyID, CathyURNID, nil, false)
	err := e.Insert(ctx, db)
	assert.NoError(t, err)
	assert.NotZero(t, e.ID())
	assert.Equal(t, e.Extra(), map[string]interface{}{})
	assert.True(t, e.OccurredOn().After(start))

	// with extra
	e2 := NewChannelEvent(MOMissEventType, Org1, TwilioChannelID, CathyID, CathyURNID, map[string]interface{}{"referral_id": "foobar"}, false)
	err = e2.Insert(ctx, db)
	assert.NoError(t, err)
	assert.NotZero(t, e2.ID())
	assert.Equal(t, e2.Extra(), map[string]interface{}{"referral_id": "foobar"})

	asJSON, err := json.Marshal(e2)
	assert.NoError(t, err)

	e3 := &ChannelEvent{}
	err = json.Unmarshal(asJSON, e3)
	assert.NoError(t, err)
	assert.Equal(t, e2.Extra(), e3.Extra())
	assert.True(t, e.OccurredOn().After(start))
}
