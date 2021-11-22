package models

import (
	"testing"
	"time"

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

func TestInvalidChannelExternalID(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()
	testsuite.Reset()

	wrongEID := "wrong_id"

	_, err := InsertIVRConnection(ctx, db, Org1, TwilioChannelID, NilStartID, CathyID, CathyURNID, ConnectionDirectionOut, ConnectionStatusPending, "")
	assert.NoError(t, err)

	_, err = SelectChannelConnectionByExternalID(ctx, db, TwilioChannelID,"V", wrongEID)
	assert.Error(t, err, "unable to load channel connection with external id: %s", wrongEID)
}

func TestUpdateStatus(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()
	testsuite.Reset()

	expectedEndTime := time.Now()
	expectedDuration1 := 5
	expectedDuration2 := 0

	conn, err := InsertIVRConnection(ctx, db, Org1, TwilioChannelID, NilStartID, CathyID, CathyURNID, ConnectionDirectionOut, ConnectionStatusPending, "")
	assert.NoError(t, err)
	assert.Equal(t, conn.Status(), ConnectionStatusPending)

	err = conn.UpdateStatus(ctx, db, ConnectionStatusQueued, expectedDuration1, expectedEndTime)
	assert.NoError(t, err)

	assert.Equal(t, conn.Status(), ConnectionStatusQueued)
	assert.Equal(t, conn.c.EndedOn, &expectedEndTime)
	assert.Equal(t, conn.c.Duration, expectedDuration1)

	err = conn.UpdateStatus(ctx, db, ConnectionStatusInProgress, expectedDuration2, time.Now())
	assert.NoError(t, err)
	assert.Equal(t, conn.Status(), ConnectionStatusInProgress)
	assert.Equal(t, conn.c.EndedOn, &expectedEndTime) // unchanged
	assert.NotEqual(t, conn.c.Duration, expectedDuration2)  // unchanged
	assert.Equal(t, conn.c.Duration, expectedDuration1)
}

func TestLoadChannelConnectionsToRetry(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()
	testsuite.Reset()

	limit := 2

	yesterday := time.Now().AddDate(0, 0, -1)

	conn1, err := InsertIVRConnection(ctx, db, Org1, NexmoChannelID, NilStartID, CathyID, CathyURNID, ConnectionDirectionOut, ConnectionStatusQueued, "")
	assert.NoError(t, err)

	_, err = InsertIVRConnection(ctx, db, Org1, NexmoChannelID, NilStartID, CathyID, CathyURNID, ConnectionDirectionOut, ConnectionStatusPending, "")
	assert.NoError(t, err)

	conns, err := LoadChannelConnectionsToRetry(ctx, db, limit)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(conns))

	err = conn1.MarkThrottled(ctx, db, yesterday)
	assert.NoError(t, err)

	conns, err = LoadChannelConnectionsToRetry(ctx, db, limit)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(conns))
}

func TestActiveChannelConnectionCount(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()
	testsuite.Reset()

	conn1, err := InsertIVRConnection(ctx, db, Org1, TwitterChannelID, NilStartID, CathyID, CathyURNID, ConnectionDirectionOut, ConnectionStatusPending, "")
	assert.NoError(t, err)

	conn2, err := InsertIVRConnection(ctx, db, Org1, TwitterChannelID, NilStartID, BobID, BobURNID, ConnectionDirectionOut, ConnectionStatusPending, "")
	assert.NoError(t, err)

	conn3, err := InsertIVRConnection(ctx, db, Org1, TwitterChannelID, NilStartID, AlexandriaID, AlexandriaURNID, ConnectionDirectionOut, ConnectionStatusWired, "")
	assert.NoError(t, err)

	count, err := ActiveChannelConnectionCount(ctx, db, TwitterChannelID)
	assert.NoError(t, err)
	assert.Equal(t, 1, count)

	err = conn1.MarkThrottled(ctx, db, time.Now())
	assert.NoError(t, err)

	err = conn2.MarkBusy(ctx, db, time.Now())
	assert.NoError(t, err)

	count, err = ActiveChannelConnectionCount(ctx, db, TwitterChannelID)
	assert.NoError(t, err)
	assert.Equal(t, 1, count)

	err = conn1.MarkStarted(ctx, db, time.Now())
	assert.NoError(t, err)

	err = conn2.MarkErrored(ctx, db, time.Now(), 1)
	assert.NoError(t, err)

	count, err = ActiveChannelConnectionCount(ctx, db, TwitterChannelID)
	assert.NoError(t, err)
	assert.Equal(t, 2, count)

	err = conn3.MarkFailed(ctx, db, time.Now())
	assert.NoError(t, err)

	count, err = ActiveChannelConnectionCount(ctx, db, TwitterChannelID)
	assert.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestUpdateChannelConnectionStatuses(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()
	testsuite.Reset()

	var connectionIDs []ConnectionID

	err := UpdateChannelConnectionStatuses(ctx, db, connectionIDs, ConnectionStatusInProgress)
	assert.NoError(t, err)

	conn1, err := InsertIVRConnection(ctx, db, Org1, TwilioChannelID, NilStartID, CathyID, CathyURNID, ConnectionDirectionOut, ConnectionStatusPending, "")
	assert.NoError(t, err)

	conn2, err := InsertIVRConnection(ctx, db, Org1, TwilioChannelID, NilStartID, BobID, BobURNID, ConnectionDirectionOut, ConnectionStatusPending, "")
	assert.NoError(t, err)

	conn3, err := InsertIVRConnection(ctx, db, Org1, TwilioChannelID, NilStartID, AlexandriaID, AlexandriaURNID, ConnectionDirectionOut, ConnectionStatusInProgress, "")
	assert.NoError(t, err)

	count, err := ActiveChannelConnectionCount(ctx, db, TwilioChannelID)
	assert.NoError(t, err)
	assert.Equal(t, 1, count)

	connectionIDs = append(connectionIDs, conn1.ID(), conn2.ID(), conn3.ID())

	err = UpdateChannelConnectionStatuses(ctx, db, connectionIDs, ConnectionStatusInProgress)
	assert.NoError(t, err)

	count, err = ActiveChannelConnectionCount(ctx, db, TwilioChannelID)
	assert.NoError(t, err)
	assert.Equal(t, 3, count)
}
