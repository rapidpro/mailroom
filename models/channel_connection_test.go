package models

import (
	"testing"

	"github.com/nyaruka/mailroom/testsuite"
	"github.com/stretchr/testify/assert"
)

func TestChannelConnections(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()

	session, err := CreateIVRConnection(ctx, db, Org1, Channel1, Cathy, CathyURNID, ConnectionDirectionOut, ConnectionStatusPending, "")
	assert.NoError(t, err)

	assert.NotEqual(t, ConnectionID(0), session.ID())

	err = session.UpdateExternalID(ctx, db, "test1")
	assert.NoError(t, err)

	testsuite.AssertQueryCount(t, db, `SELECT count(*) from channels_channelconnection where external_id = 'test1' AND id = $1`, []interface{}{session.ID()}, 1)
}
