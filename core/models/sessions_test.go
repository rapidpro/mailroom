package models_test

import (
	"context"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/test"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSessionCreationAndUpdating(t *testing.T) {
	ctx, rt, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetData)

	testFlows := testdata.ImportFlows(db, testdata.Org1, "testdata/session_test_flows.json")
	flow := testFlows[0]

	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshFlows)
	require.NoError(t, err)

	modelContact, _ := testdata.Bob.Load(db, oa)

	sa, flowSession, sprint1 := test.NewSessionBuilder().WithAssets(oa.SessionAssets()).WithFlow(flow.UUID).
		WithContact(testdata.Bob.UUID, flows.ContactID(testdata.Bob.ID), "Bob", "eng", "").MustBuild()

	tx := db.MustBegin()

	hookCalls := 0
	hook := func(context.Context, *sqlx.Tx, *redis.Pool, *models.OrgAssets, []*models.Session) error {
		hookCalls++
		return nil
	}

	modelSessions, err := models.InsertSessions(ctx, rt, tx, oa, []flows.Session{flowSession}, []flows.Sprint{sprint1}, []*models.Contact{modelContact}, hook)
	require.NoError(t, err)
	assert.Equal(t, 1, hookCalls)

	require.NoError(t, tx.Commit())

	session := modelSessions[0]

	assert.Equal(t, models.FlowTypeMessaging, session.SessionType())
	assert.Equal(t, testdata.Bob.ID, session.ContactID())
	assert.Equal(t, models.SessionStatusWaiting, session.Status())
	assert.Equal(t, flow.ID, session.CurrentFlowID())
	assert.NotNil(t, session.CreatedOn())
	assert.Nil(t, session.EndedOn())
	assert.False(t, session.Responded())
	assert.NotNil(t, session.WaitStartedOn())
	assert.NotNil(t, session.WaitExpiresOn())
	assert.False(t, session.WaitResumeOnExpire())
	assert.NotNil(t, session.Timeout())

	// check that matches what is in the db
	assertdb.Query(t, db, `SELECT status, session_type, current_flow_id, responded, ended_on, wait_resume_on_expire FROM flows_flowsession`).
		Columns(map[string]interface{}{
			"status": "W", "session_type": "M", "current_flow_id": int64(flow.ID), "responded": false, "ended_on": nil, "wait_resume_on_expire": false,
		})

	// reload contact and check current flow is set
	modelContact, _ = testdata.Bob.Load(db, oa)
	assert.Equal(t, flow.ID, modelContact.CurrentFlowID())

	flowSession, err = session.FlowSession(rt.Config, oa.SessionAssets(), oa.Env())
	require.NoError(t, err)

	flowSession, sprint2, err := test.ResumeSession(flowSession, sa, "no")
	require.NoError(t, err)

	tx = db.MustBegin()

	err = session.Update(ctx, rt, tx, oa, flowSession, sprint2, modelContact, hook)
	require.NoError(t, err)
	assert.Equal(t, 2, hookCalls)

	require.NoError(t, tx.Commit())

	assert.Equal(t, models.SessionStatusWaiting, session.Status())
	assert.Equal(t, flow.ID, session.CurrentFlowID())
	assert.True(t, session.Responded())
	assert.NotNil(t, session.WaitStartedOn())
	assert.NotNil(t, session.WaitExpiresOn())
	assert.False(t, session.WaitResumeOnExpire())
	assert.Nil(t, session.Timeout()) // this wait doesn't have a timeout

	flowSession, err = session.FlowSession(rt.Config, oa.SessionAssets(), oa.Env())
	require.NoError(t, err)

	flowSession, sprint3, err := test.ResumeSession(flowSession, sa, "yes")
	require.NoError(t, err)

	tx = db.MustBegin()

	err = session.Update(ctx, rt, tx, oa, flowSession, sprint3, modelContact, hook)
	require.NoError(t, err)
	assert.Equal(t, 3, hookCalls)

	require.NoError(t, tx.Commit())

	assert.Equal(t, models.SessionStatusCompleted, session.Status())
	assert.Equal(t, models.NilFlowID, session.CurrentFlowID()) // no longer "in" a flow
	assert.True(t, session.Responded())
	assert.NotNil(t, session.CreatedOn())
	assert.Nil(t, session.WaitStartedOn())
	assert.Nil(t, session.WaitExpiresOn())
	assert.False(t, session.WaitResumeOnExpire())
	assert.Nil(t, session.Timeout())
	assert.NotNil(t, session.EndedOn())

	// check that matches what is in the db
	assertdb.Query(t, db, `SELECT status, session_type, current_flow_id, responded FROM flows_flowsession`).
		Columns(map[string]interface{}{"status": "C", "session_type": "M", "current_flow_id": nil, "responded": true})

	assertdb.Query(t, db, `SELECT current_flow_id FROM contacts_contact WHERE id = $1`, testdata.Bob.ID).Returns(nil)

	// reload contact and check current flow is cleared
	modelContact, _ = testdata.Bob.Load(db, oa)
	assert.Equal(t, models.NilFlowID, modelContact.CurrentFlowID())
}

func TestSingleSprintSession(t *testing.T) {
	ctx, rt, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetData)

	testFlows := testdata.ImportFlows(db, testdata.Org1, "testdata/session_test_flows.json")
	flow := testFlows[1]

	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshFlows)
	require.NoError(t, err)

	modelContact, _ := testdata.Bob.Load(db, oa)

	_, flowSession, sprint1 := test.NewSessionBuilder().WithAssets(oa.SessionAssets()).WithFlow(flow.UUID).
		WithContact(testdata.Bob.UUID, flows.ContactID(testdata.Bob.ID), "Bob", "eng", "").MustBuild()

	tx := db.MustBegin()

	hookCalls := 0
	hook := func(context.Context, *sqlx.Tx, *redis.Pool, *models.OrgAssets, []*models.Session) error {
		hookCalls++
		return nil
	}

	modelSessions, err := models.InsertSessions(ctx, rt, tx, oa, []flows.Session{flowSession}, []flows.Sprint{sprint1}, []*models.Contact{modelContact}, hook)
	require.NoError(t, err)
	assert.Equal(t, 1, hookCalls)

	require.NoError(t, tx.Commit())

	session := modelSessions[0]

	assert.Equal(t, models.FlowTypeMessaging, session.SessionType())
	assert.Equal(t, testdata.Bob.ID, session.ContactID())
	assert.Equal(t, models.SessionStatusCompleted, session.Status())
	assert.Equal(t, models.NilFlowID, session.CurrentFlowID())
	assert.NotNil(t, session.CreatedOn())
	assert.NotNil(t, session.EndedOn())
	assert.False(t, session.Responded())
	assert.Nil(t, session.WaitStartedOn())
	assert.Nil(t, session.WaitExpiresOn())
	assert.Nil(t, session.Timeout())

	// check that matches what is in the db
	assertdb.Query(t, db, `SELECT status, session_type, current_flow_id, responded FROM flows_flowsession`).
		Columns(map[string]interface{}{"status": "C", "session_type": "M", "current_flow_id": nil, "responded": false})
}

func TestSessionWithSubflows(t *testing.T) {
	ctx, rt, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetData)

	testFlows := testdata.ImportFlows(db, testdata.Org1, "testdata/session_test_flows.json")
	parent, child := testFlows[2], testFlows[3]

	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshFlows)
	require.NoError(t, err)

	modelContact, _ := testdata.Cathy.Load(db, oa)

	sa, flowSession, sprint1 := test.NewSessionBuilder().WithAssets(oa.SessionAssets()).WithFlow(parent.UUID).
		WithContact(testdata.Cathy.UUID, flows.ContactID(testdata.Cathy.ID), "Cathy", "eng", "").MustBuild()

	tx := db.MustBegin()

	hookCalls := 0
	hook := func(context.Context, *sqlx.Tx, *redis.Pool, *models.OrgAssets, []*models.Session) error {
		hookCalls++
		return nil
	}

	modelSessions, err := models.InsertSessions(ctx, rt, tx, oa, []flows.Session{flowSession}, []flows.Sprint{sprint1}, []*models.Contact{modelContact}, hook)
	require.NoError(t, err)
	assert.Equal(t, 1, hookCalls)

	require.NoError(t, tx.Commit())

	session := modelSessions[0]

	assert.Equal(t, models.FlowTypeMessaging, session.SessionType())
	assert.Equal(t, testdata.Cathy.ID, session.ContactID())
	assert.Equal(t, models.SessionStatusWaiting, session.Status())
	assert.Equal(t, child.ID, session.CurrentFlowID())
	assert.NotNil(t, session.CreatedOn())
	assert.Nil(t, session.EndedOn())
	assert.False(t, session.Responded())
	assert.NotNil(t, session.WaitStartedOn())
	assert.NotNil(t, session.WaitExpiresOn())
	assert.True(t, session.WaitResumeOnExpire()) // because we have a parent
	assert.Nil(t, session.Timeout())

	// check that matches what is in the db
	assertdb.Query(t, db, `SELECT status, session_type, current_flow_id, responded, ended_on, wait_resume_on_expire FROM flows_flowsession`).
		Columns(map[string]interface{}{
			"status": "W", "session_type": "M", "current_flow_id": int64(child.ID), "responded": false, "ended_on": nil, "wait_resume_on_expire": true,
		})

	flowSession, err = session.FlowSession(rt.Config, oa.SessionAssets(), oa.Env())
	require.NoError(t, err)

	flowSession, sprint2, err := test.ResumeSession(flowSession, sa, "yes")
	require.NoError(t, err)

	tx = db.MustBegin()

	err = session.Update(ctx, rt, tx, oa, flowSession, sprint2, modelContact, hook)
	require.NoError(t, err)
	assert.Equal(t, 2, hookCalls)

	require.NoError(t, tx.Commit())

	assert.Equal(t, models.SessionStatusCompleted, session.Status())
	assert.Equal(t, models.NilFlowID, session.CurrentFlowID())
	assert.True(t, session.Responded())
	assert.Nil(t, session.WaitStartedOn())
	assert.Nil(t, session.WaitExpiresOn())
	assert.False(t, session.WaitResumeOnExpire())
	assert.Nil(t, session.Timeout())
}

func TestSessionFailedStart(t *testing.T) {
	ctx, rt, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetData)

	testFlows := testdata.ImportFlows(db, testdata.Org1, "testdata/ping_pong.json")
	ping, pong := testFlows[0], testFlows[1]

	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshFlows)
	require.NoError(t, err)

	modelContact, _ := testdata.Cathy.Load(db, oa)

	_, flowSession, sprint1 := test.NewSessionBuilder().WithAssets(oa.SessionAssets()).WithFlow(ping.UUID).
		WithContact(testdata.Cathy.UUID, flows.ContactID(testdata.Cathy.ID), "Cathy", "eng", "").MustBuild()

	tx := db.MustBegin()

	hookCalls := 0
	hook := func(context.Context, *sqlx.Tx, *redis.Pool, *models.OrgAssets, []*models.Session) error {
		hookCalls++
		return nil
	}

	modelSessions, err := models.InsertSessions(ctx, rt, tx, oa, []flows.Session{flowSession}, []flows.Sprint{sprint1}, []*models.Contact{modelContact}, hook)
	require.NoError(t, err)
	assert.Equal(t, 1, hookCalls)

	require.NoError(t, tx.Commit())

	session := modelSessions[0]

	assert.Equal(t, models.FlowTypeMessaging, session.SessionType())
	assert.Equal(t, testdata.Cathy.ID, session.ContactID())
	assert.Equal(t, models.SessionStatusFailed, session.Status())
	assert.Equal(t, models.NilFlowID, session.CurrentFlowID())
	assert.Nil(t, session.WaitExpiresOn())
	assert.NotNil(t, session.EndedOn())

	// check that matches what is in the db
	assertdb.Query(t, db, `SELECT status, session_type, current_flow_id, responded FROM flows_flowsession`).
		Columns(map[string]interface{}{"status": "F", "session_type": "M", "current_flow_id": nil, "responded": false})
	assertdb.Query(t, db, `SELECT count(*) FROM flows_flowsession WHERE ended_on IS NOT NULL`).Returns(1)

	// check the state of all the created runs
	assertdb.Query(t, db, `SELECT count(*) FROM flows_flowrun`).Returns(101)
	assertdb.Query(t, db, `SELECT count(*) FROM flows_flowrun WHERE flow_id = $1`, ping.ID).Returns(51)
	assertdb.Query(t, db, `SELECT count(*) FROM flows_flowrun WHERE flow_id = $1`, pong.ID).Returns(50)
	assertdb.Query(t, db, `SELECT count(*) FROM flows_flowrun WHERE status = 'F' AND exited_on IS NOT NULL`).Returns(101)
}

func TestInterruptSessionsForContacts(t *testing.T) {
	ctx, _, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetData)

	session1ID, _ := insertSessionAndRun(db, testdata.Cathy, models.FlowTypeMessaging, models.SessionStatusCompleted, testdata.Favorites, models.NilConnectionID)
	session2ID, run2ID := insertSessionAndRun(db, testdata.Cathy, models.FlowTypeVoice, models.SessionStatusWaiting, testdata.Favorites, models.NilConnectionID)
	session3ID, _ := insertSessionAndRun(db, testdata.Bob, models.FlowTypeMessaging, models.SessionStatusWaiting, testdata.Favorites, models.NilConnectionID)
	session4ID, _ := insertSessionAndRun(db, testdata.George, models.FlowTypeMessaging, models.SessionStatusWaiting, testdata.Favorites, models.NilConnectionID)

	// noop if no contacts
	_, err := models.InterruptSessionsForContacts(ctx, db, []models.ContactID{})
	assert.NoError(t, err)

	assertSessionAndRunStatus(t, db, session1ID, models.SessionStatusCompleted)
	assertSessionAndRunStatus(t, db, session2ID, models.SessionStatusWaiting)
	assertSessionAndRunStatus(t, db, session3ID, models.SessionStatusWaiting)
	assertSessionAndRunStatus(t, db, session4ID, models.SessionStatusWaiting)

	count, err := models.InterruptSessionsForContacts(ctx, db, []models.ContactID{testdata.Cathy.ID, testdata.Bob.ID, testdata.Alexandria.ID})
	assert.Equal(t, 2, count)
	assert.NoError(t, err)

	assertSessionAndRunStatus(t, db, session1ID, models.SessionStatusCompleted) // wasn't waiting
	assertSessionAndRunStatus(t, db, session2ID, models.SessionStatusInterrupted)
	assertSessionAndRunStatus(t, db, session3ID, models.SessionStatusInterrupted)
	assertSessionAndRunStatus(t, db, session4ID, models.SessionStatusWaiting) // contact not included

	// check other columns are correct on interrupted session, run and contact
	assertdb.Query(t, db, `SELECT count(*) FROM flows_flowsession WHERE ended_on IS NOT NULL AND wait_started_on IS NULL AND wait_expires_on IS NULL AND timeout_on IS NULL AND current_flow_id IS NULL AND id = $1`, session2ID).Returns(1)
	assertdb.Query(t, db, `SELECT status FROM flows_flowrun WHERE id = $1`, run2ID).Columns(map[string]interface{}{"status": "I"})
	assertdb.Query(t, db, `SELECT current_flow_id FROM contacts_contact WHERE id = $1`, testdata.Cathy.ID).Returns(nil)
}

func TestInterruptSessionsForContactsTx(t *testing.T) {
	ctx, _, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetData)

	session1ID, _ := insertSessionAndRun(db, testdata.Cathy, models.FlowTypeMessaging, models.SessionStatusCompleted, testdata.Favorites, models.NilConnectionID)
	session2ID, run2ID := insertSessionAndRun(db, testdata.Cathy, models.FlowTypeVoice, models.SessionStatusWaiting, testdata.Favorites, models.NilConnectionID)
	session3ID, _ := insertSessionAndRun(db, testdata.Bob, models.FlowTypeMessaging, models.SessionStatusWaiting, testdata.Favorites, models.NilConnectionID)
	session4ID, _ := insertSessionAndRun(db, testdata.George, models.FlowTypeMessaging, models.SessionStatusWaiting, testdata.Favorites, models.NilConnectionID)

	tx := db.MustBegin()

	// noop if no contacts
	err := models.InterruptSessionsForContactsTx(ctx, tx, []models.ContactID{})
	require.NoError(t, err)

	require.NoError(t, tx.Commit())

	assertSessionAndRunStatus(t, db, session1ID, models.SessionStatusCompleted)
	assertSessionAndRunStatus(t, db, session2ID, models.SessionStatusWaiting)
	assertSessionAndRunStatus(t, db, session3ID, models.SessionStatusWaiting)
	assertSessionAndRunStatus(t, db, session4ID, models.SessionStatusWaiting)

	tx = db.MustBegin()

	err = models.InterruptSessionsForContactsTx(ctx, tx, []models.ContactID{testdata.Cathy.ID, testdata.Bob.ID})
	require.NoError(t, err)

	require.NoError(t, tx.Commit())

	assertSessionAndRunStatus(t, db, session1ID, models.SessionStatusCompleted) // wasn't waiting
	assertSessionAndRunStatus(t, db, session2ID, models.SessionStatusInterrupted)
	assertSessionAndRunStatus(t, db, session3ID, models.SessionStatusInterrupted)
	assertSessionAndRunStatus(t, db, session4ID, models.SessionStatusWaiting) // contact not included

	// check other columns are correct on interrupted session, run and contact
	assertdb.Query(t, db, `SELECT count(*) FROM flows_flowsession WHERE ended_on IS NOT NULL AND wait_started_on IS NULL AND wait_expires_on IS NULL AND timeout_on IS NULL AND current_flow_id IS NULL AND id = $1`, session2ID).Returns(1)
	assertdb.Query(t, db, `SELECT status FROM flows_flowrun WHERE id = $1`, run2ID).Columns(map[string]interface{}{"status": "I"})
	assertdb.Query(t, db, `SELECT current_flow_id FROM contacts_contact WHERE id = $1`, testdata.Cathy.ID).Returns(nil)
}

func TestInterruptSessionsForChannels(t *testing.T) {
	ctx, _, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetData)

	cathy1ConnectionID := testdata.InsertConnection(db, testdata.Org1, testdata.TwilioChannel, testdata.Cathy)
	cathy2ConnectionID := testdata.InsertConnection(db, testdata.Org1, testdata.TwilioChannel, testdata.Cathy)
	bobConnectionID := testdata.InsertConnection(db, testdata.Org1, testdata.TwilioChannel, testdata.Bob)
	georgeConnectionID := testdata.InsertConnection(db, testdata.Org1, testdata.VonageChannel, testdata.George)

	session1ID, _ := insertSessionAndRun(db, testdata.Cathy, models.FlowTypeMessaging, models.SessionStatusCompleted, testdata.Favorites, cathy1ConnectionID)
	session2ID, _ := insertSessionAndRun(db, testdata.Cathy, models.FlowTypeMessaging, models.SessionStatusWaiting, testdata.Favorites, cathy2ConnectionID)
	session3ID, _ := insertSessionAndRun(db, testdata.Bob, models.FlowTypeMessaging, models.SessionStatusWaiting, testdata.Favorites, bobConnectionID)
	session4ID, _ := insertSessionAndRun(db, testdata.George, models.FlowTypeMessaging, models.SessionStatusWaiting, testdata.Favorites, georgeConnectionID)

	// noop if no channels
	err := models.InterruptSessionsForChannels(ctx, db, []models.ChannelID{})
	require.NoError(t, err)

	assertSessionAndRunStatus(t, db, session1ID, models.SessionStatusCompleted)
	assertSessionAndRunStatus(t, db, session2ID, models.SessionStatusWaiting)
	assertSessionAndRunStatus(t, db, session3ID, models.SessionStatusWaiting)
	assertSessionAndRunStatus(t, db, session4ID, models.SessionStatusWaiting)

	err = models.InterruptSessionsForChannels(ctx, db, []models.ChannelID{testdata.TwilioChannel.ID})
	require.NoError(t, err)

	assertSessionAndRunStatus(t, db, session1ID, models.SessionStatusCompleted) // wasn't waiting
	assertSessionAndRunStatus(t, db, session2ID, models.SessionStatusInterrupted)
	assertSessionAndRunStatus(t, db, session3ID, models.SessionStatusInterrupted)
	assertSessionAndRunStatus(t, db, session4ID, models.SessionStatusWaiting) // channel not included

	// check other columns are correct on interrupted session and contact
	assertdb.Query(t, db, `SELECT count(*) FROM flows_flowsession WHERE ended_on IS NOT NULL AND wait_started_on IS NULL AND wait_expires_on IS NULL AND timeout_on IS NULL AND current_flow_id IS NULL AND id = $1`, session2ID).Returns(1)
	assertdb.Query(t, db, `SELECT current_flow_id FROM contacts_contact WHERE id = $1`, testdata.Cathy.ID).Returns(nil)
}

func TestInterruptSessionsForFlows(t *testing.T) {
	ctx, _, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetData)

	cathy1ConnectionID := testdata.InsertConnection(db, testdata.Org1, testdata.TwilioChannel, testdata.Cathy)
	cathy2ConnectionID := testdata.InsertConnection(db, testdata.Org1, testdata.TwilioChannel, testdata.Cathy)
	bobConnectionID := testdata.InsertConnection(db, testdata.Org1, testdata.TwilioChannel, testdata.Bob)
	georgeConnectionID := testdata.InsertConnection(db, testdata.Org1, testdata.VonageChannel, testdata.George)

	session1ID, _ := insertSessionAndRun(db, testdata.Cathy, models.FlowTypeMessaging, models.SessionStatusCompleted, testdata.Favorites, cathy1ConnectionID)
	session2ID, _ := insertSessionAndRun(db, testdata.Cathy, models.FlowTypeMessaging, models.SessionStatusWaiting, testdata.Favorites, cathy2ConnectionID)
	session3ID, _ := insertSessionAndRun(db, testdata.Bob, models.FlowTypeMessaging, models.SessionStatusWaiting, testdata.Favorites, bobConnectionID)
	session4ID, _ := insertSessionAndRun(db, testdata.George, models.FlowTypeMessaging, models.SessionStatusWaiting, testdata.PickANumber, georgeConnectionID)

	// noop if no flows
	err := models.InterruptSessionsForFlows(ctx, db, []models.FlowID{})
	require.NoError(t, err)

	assertSessionAndRunStatus(t, db, session1ID, models.SessionStatusCompleted)
	assertSessionAndRunStatus(t, db, session2ID, models.SessionStatusWaiting)
	assertSessionAndRunStatus(t, db, session3ID, models.SessionStatusWaiting)
	assertSessionAndRunStatus(t, db, session4ID, models.SessionStatusWaiting)

	err = models.InterruptSessionsForFlows(ctx, db, []models.FlowID{testdata.Favorites.ID})
	require.NoError(t, err)

	assertSessionAndRunStatus(t, db, session1ID, models.SessionStatusCompleted) // wasn't waiting
	assertSessionAndRunStatus(t, db, session2ID, models.SessionStatusInterrupted)
	assertSessionAndRunStatus(t, db, session3ID, models.SessionStatusInterrupted)
	assertSessionAndRunStatus(t, db, session4ID, models.SessionStatusWaiting) // flow not included

	// check other columns are correct on interrupted session and contact
	assertdb.Query(t, db, `SELECT count(*) FROM flows_flowsession WHERE ended_on IS NOT NULL AND wait_started_on IS NULL AND wait_expires_on IS NULL AND timeout_on IS NULL AND current_flow_id IS NULL AND id = $1`, session2ID).Returns(1)
	assertdb.Query(t, db, `SELECT current_flow_id FROM contacts_contact WHERE id = $1`, testdata.Cathy.ID).Returns(nil)
}

func TestGetSessionWaitExpiresOn(t *testing.T) {
	ctx, _, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetData)

	s1Expires := time.Date(2022, 1, 26, 13, 28, 30, 0, time.UTC)
	s1ID := testdata.InsertWaitingSession(db, testdata.Org1, testdata.Cathy, models.FlowTypeMessaging, testdata.Favorites, models.NilConnectionID, time.Now(), s1Expires, true, nil)

	s1Actual, err := models.GetSessionWaitExpiresOn(ctx, db, s1ID)
	assert.NoError(t, err)
	assert.Equal(t, s1Expires, *s1Actual)

	// for a non-waiting session, should return nil
	s2ID := testdata.InsertFlowSession(db, testdata.Org1, testdata.Cathy, models.FlowTypeMessaging, models.SessionStatusCompleted, testdata.Favorites, models.NilConnectionID)

	s2Actual, err := models.GetSessionWaitExpiresOn(ctx, db, s2ID)
	assert.NoError(t, err)
	assert.Nil(t, s2Actual)
}

func TestClearWaitTimeout(t *testing.T) {
	ctx, rt, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetData)

	oa := testdata.Org1.Load(rt)

	_, cathy := testdata.Cathy.Load(db, oa)

	expiresOn := time.Now().Add(time.Hour)
	timeoutOn := time.Now().Add(time.Minute)
	testdata.InsertWaitingSession(db, testdata.Org1, testdata.Cathy, models.FlowTypeMessaging, testdata.Favorites, models.NilConnectionID, time.Now(), expiresOn, true, &timeoutOn)

	session, err := models.FindWaitingSessionForContact(ctx, db, nil, oa, models.FlowTypeMessaging, cathy)
	require.NoError(t, err)

	// can be called without db connection to clear without updating db
	session.ClearWaitTimeout(ctx, nil)
	assert.Nil(t, session.WaitTimeoutOn())
	assert.NotNil(t, session.WaitExpiresOn()) // unaffected

	// and called with one to clear in the database as well
	session.ClearWaitTimeout(ctx, db)
	assert.Nil(t, session.WaitTimeoutOn())

	assertdb.Query(t, db, `SELECT timeout_on FROM flows_flowsession WHERE id = $1`, session.ID()).Returns(nil)
}

func insertSessionAndRun(db *sqlx.DB, contact *testdata.Contact, sessionType models.FlowType, status models.SessionStatus, flow *testdata.Flow, connID models.ConnectionID) (models.SessionID, models.FlowRunID) {
	// create session and add a run with same status
	sessionID := testdata.InsertFlowSession(db, testdata.Org1, contact, sessionType, status, flow, connID)
	runID := testdata.InsertFlowRun(db, testdata.Org1, sessionID, contact, flow, models.RunStatus(status))

	// mark contact as being in that flow
	db.MustExec(`UPDATE contacts_contact SET current_flow_id = $2 WHERE id = $1`, contact.ID, flow.ID)

	return sessionID, runID
}

func assertSessionAndRunStatus(t *testing.T, db *sqlx.DB, sessionID models.SessionID, status models.SessionStatus) {
	assertdb.Query(t, db, `SELECT status FROM flows_flowsession WHERE id = $1`, sessionID).Columns(map[string]interface{}{"status": string(status)})
	assertdb.Query(t, db, `SELECT status FROM flows_flowrun WHERE session_id = $1`, sessionID).Columns(map[string]interface{}{"status": string(status)})
}
