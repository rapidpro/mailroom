package models_test

import (
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/require"
)

func TestInterruptContactSessions(t *testing.T) {
	ctx, _, db, _ := testsuite.Get()

	session1ID, _ := insertSessionAndRun(db, testdata.Cathy, models.FlowTypeMessaging, models.SessionStatusCompleted, models.NilConnectionID)
	session2ID, _ := insertSessionAndRun(db, testdata.Cathy, models.FlowTypeMessaging, models.SessionStatusWaiting, models.NilConnectionID)
	session3ID, _ := insertSessionAndRun(db, testdata.Bob, models.FlowTypeMessaging, models.SessionStatusWaiting, models.NilConnectionID)
	session4ID, _ := insertSessionAndRun(db, testdata.George, models.FlowTypeMessaging, models.SessionStatusWaiting, models.NilConnectionID)

	// noop if no contacts
	err := models.InterruptContactSessions(ctx, db, []models.ContactID{})
	require.NoError(t, err)

	assertSessionAndRunStatus(t, db, session1ID, models.SessionStatusCompleted)
	assertSessionAndRunStatus(t, db, session2ID, models.SessionStatusWaiting)
	assertSessionAndRunStatus(t, db, session3ID, models.SessionStatusWaiting)
	assertSessionAndRunStatus(t, db, session4ID, models.SessionStatusWaiting)

	err = models.InterruptContactSessions(ctx, db, []models.ContactID{testdata.Cathy.ID, testdata.Bob.ID})
	require.NoError(t, err)

	assertSessionAndRunStatus(t, db, session1ID, models.SessionStatusCompleted) // wasn't waiting
	assertSessionAndRunStatus(t, db, session2ID, models.SessionStatusInterrupted)
	assertSessionAndRunStatus(t, db, session3ID, models.SessionStatusInterrupted)
	assertSessionAndRunStatus(t, db, session4ID, models.SessionStatusWaiting) // contact not included

	// check other columns are correct on interrupted session
	assertdb.Query(t, db, `SELECT count(*) FROM flows_flowsession WHERE ended_on IS NOT NULL AND wait_started_on IS NULL AND wait_expires_on IS NULL AND timeout_on IS NULL AND current_flow_id IS NULL AND id = $1`, session2ID).Returns(1)
}

func TestInterruptContactSessionsOfType(t *testing.T) {
	ctx, _, db, _ := testsuite.Get()

	session1ID, _ := insertSessionAndRun(db, testdata.Cathy, models.FlowTypeMessaging, models.SessionStatusCompleted, models.NilConnectionID)
	session2ID, _ := insertSessionAndRun(db, testdata.Cathy, models.FlowTypeMessaging, models.SessionStatusWaiting, models.NilConnectionID)
	session3ID, _ := insertSessionAndRun(db, testdata.Bob, models.FlowTypeMessaging, models.SessionStatusWaiting, models.NilConnectionID)
	session4ID, _ := insertSessionAndRun(db, testdata.George, models.FlowTypeVoice, models.SessionStatusWaiting, models.NilConnectionID)

	err := models.InterruptContactSessionsOfType(ctx, db, []models.ContactID{testdata.Cathy.ID, testdata.Bob.ID, testdata.George.ID}, models.FlowTypeMessaging)
	require.NoError(t, err)

	assertSessionAndRunStatus(t, db, session1ID, models.SessionStatusCompleted) // wasn't waiting
	assertSessionAndRunStatus(t, db, session2ID, models.SessionStatusInterrupted)
	assertSessionAndRunStatus(t, db, session3ID, models.SessionStatusInterrupted)
	assertSessionAndRunStatus(t, db, session4ID, models.SessionStatusWaiting) // wrong type

	// check other columns are correct on interrupted session
	assertdb.Query(t, db, `SELECT count(*) FROM flows_flowsession WHERE ended_on IS NOT NULL AND wait_started_on IS NULL AND wait_expires_on IS NULL AND timeout_on IS NULL AND current_flow_id IS NULL AND id = $1`, session2ID).Returns(1)
}

func TestInterruptChannelSessions(t *testing.T) {
	ctx, _, db, _ := testsuite.Get()

	cathy1ConnectionID := testdata.InsertConnection(db, testdata.Org1, testdata.TwilioChannel, testdata.Cathy)
	cathy2ConnectionID := testdata.InsertConnection(db, testdata.Org1, testdata.TwilioChannel, testdata.Cathy)
	bobConnectionID := testdata.InsertConnection(db, testdata.Org1, testdata.TwilioChannel, testdata.Bob)
	georgeConnectionID := testdata.InsertConnection(db, testdata.Org1, testdata.VonageChannel, testdata.George)

	session1ID, _ := insertSessionAndRun(db, testdata.Cathy, models.FlowTypeMessaging, models.SessionStatusCompleted, cathy1ConnectionID)
	session2ID, _ := insertSessionAndRun(db, testdata.Cathy, models.FlowTypeMessaging, models.SessionStatusWaiting, cathy2ConnectionID)
	session3ID, _ := insertSessionAndRun(db, testdata.Bob, models.FlowTypeMessaging, models.SessionStatusWaiting, bobConnectionID)
	session4ID, _ := insertSessionAndRun(db, testdata.George, models.FlowTypeMessaging, models.SessionStatusWaiting, georgeConnectionID)

	// noop if no channels
	err := models.InterruptChannelSessions(ctx, db, []models.ChannelID{})
	require.NoError(t, err)

	assertSessionAndRunStatus(t, db, session1ID, models.SessionStatusCompleted)
	assertSessionAndRunStatus(t, db, session2ID, models.SessionStatusWaiting)
	assertSessionAndRunStatus(t, db, session3ID, models.SessionStatusWaiting)
	assertSessionAndRunStatus(t, db, session4ID, models.SessionStatusWaiting)

	err = models.InterruptChannelSessions(ctx, db, []models.ChannelID{testdata.TwilioChannel.ID})
	require.NoError(t, err)

	assertSessionAndRunStatus(t, db, session1ID, models.SessionStatusCompleted) // wasn't waiting
	assertSessionAndRunStatus(t, db, session2ID, models.SessionStatusInterrupted)
	assertSessionAndRunStatus(t, db, session3ID, models.SessionStatusInterrupted)
	assertSessionAndRunStatus(t, db, session4ID, models.SessionStatusWaiting) // channel not included

	// check other columns are correct on interrupted session
	assertdb.Query(t, db, `SELECT count(*) FROM flows_flowsession WHERE ended_on IS NOT NULL AND wait_started_on IS NULL AND wait_expires_on IS NULL AND timeout_on IS NULL AND current_flow_id IS NULL AND id = $1`, session2ID).Returns(1)
}

func insertSessionAndRun(db *sqlx.DB, contact *testdata.Contact, sessionType models.FlowType, status models.SessionStatus, connID models.ConnectionID) (models.SessionID, models.FlowRunID) {
	sessionID := testdata.InsertFlowSession(db, testdata.Org1, contact, sessionType, status, testdata.Favorites, connID, nil)
	runID := testdata.InsertFlowRun(db, testdata.Org1, sessionID, contact, testdata.Favorites, models.RunStatus(status), "", nil)
	return sessionID, runID
}

func assertSessionAndRunStatus(t *testing.T, db *sqlx.DB, sessionID models.SessionID, status models.SessionStatus) {
	assertdb.Query(t, db, `SELECT status FROM flows_flowsession WHERE id = $1`, sessionID).Columns(map[string]interface{}{"status": string(status)})
	assertdb.Query(t, db, `SELECT status FROM flows_flowrun WHERE session_id = $1`, sessionID).Columns(map[string]interface{}{"status": string(status)})
}
