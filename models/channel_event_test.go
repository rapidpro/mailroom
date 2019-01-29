package models

import (
	"testing"

	"github.com/nyaruka/mailroom/testsuite"
	"github.com/stretchr/testify/assert"
)

func TestChannelEvents(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()

	// no extra
	e := NewChannelEvent(MOMissEventType, Org1, TwilioChannelID, CathyID, CathyURNID, nil, false)
	err := e.Insert(ctx, db)
	assert.NoError(t, err)
	assert.NotZero(t, e.ID())
	assert.Equal(t, e.Extra(), map[string]string{})

	// with extra
	e2 := NewChannelEvent(MOMissEventType, Org1, TwilioChannelID, CathyID, CathyURNID, map[string]string{"referral_id": "foobar"}, false)
	err = e2.Insert(ctx, db)
	assert.NoError(t, err)
	assert.NotZero(t, e2.ID())
	assert.Equal(t, e2.Extra(), map[string]string{"referral_id": "foobar"})
}
