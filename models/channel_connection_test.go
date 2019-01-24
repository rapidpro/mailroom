package models

import (
	"testing"

	"github.com/nyaruka/mailroom/testsuite"
	"github.com/stretchr/testify/assert"
)

func TestChannelConnections(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()

	conn, err := InsertIVRConnection(ctx, db, Org1, TwilioChannelID, NilStartID, CathyID, CathyURNID, ConnectionDirectionOut, ConnectionStatusPending, "")
	assert.NoError(t, err)

	assert.NotEqual(t, ConnectionID(0), conn.ID())

	err = conn.UpdateExternalID(ctx, db, "test1")
	assert.NoError(t, err)

	testsuite.AssertQueryCount(t, db, `SELECT count(*) from channels_channelconnection where external_id = 'test1' AND id = $1`, []interface{}{conn.ID()}, 1)

	conn2, err := SelectChannelConnection(ctx, db, conn.ID())
	assert.NoError(t, err)
	assert.Equal(t, "test1", conn2.ExternalID())
}
