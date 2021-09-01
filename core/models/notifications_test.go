package models_test

import (
	"context"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNotifications(t *testing.T) {
	ctx, _, db, _ := testsuite.Reset()

	oa, err := models.GetOrgAssets(ctx, db, testdata.Org1.ID)
	require.NoError(t, err)

	// open an unassigned ticket by a flow (i.e. no user)
	ticket, openedEvent := openTicket(ctx, db, nil, nil)
	err = models.LogTicketsOpened(ctx, db, oa, []*models.TicketEvent{openedEvent})
	require.NoError(t, err)

	// check that all assignable users are notified
	log := assertNotifications(t, ctx, db, models.LogTypeTicketOpened, nil, []*testdata.User{testdata.Admin, testdata.Editor, testdata.Agent})
	assert.Equal(t, ticket.ID(), log.TicketID)
	assert.Equal(t, openedEvent.ID(), log.TicketEventID)

	// open an unassigned ticket by a user
	_, openedEvent = openTicket(ctx, db, testdata.Editor, nil)
	err = models.LogTicketsOpened(ctx, db, oa, []*models.TicketEvent{openedEvent})
	require.NoError(t, err)

	// check that all assignable users are notified except the user who opened the ticket
	assertNotifications(t, ctx, db, models.LogTypeTicketOpened, testdata.Editor, []*testdata.User{testdata.Admin, testdata.Agent})

	// open an assigned ticket
	_, openedEvent = openTicket(ctx, db, nil, testdata.Agent)
	err = models.LogTicketsOpened(ctx, db, oa, []*models.TicketEvent{openedEvent})
	require.NoError(t, err)

	// check that we don't notify any users
	assertNotifications(t, ctx, db, models.LogTypeTicketOpened, nil, nil)
}

func assertNotifications(t *testing.T, ctx context.Context, db *sqlx.DB, expectedType models.LogType, expectedCreatedBy *testdata.User, expectedNotified []*testdata.User) *models.Log {
	// check last log
	log := &models.Log{}
	err := db.GetContext(ctx, log, `SELECT id, org_id, log_type, created_by_id, ticket_id, ticket_event_id FROM notifications_log ORDER BY id DESC LIMIT 1`)
	require.NoError(t, err)

	assert.Equal(t, expectedType, log.LogType, "log type mismatch")
	assert.Equal(t, expectedCreatedBy.SafeID(), log.CreatedByID, "log created by mismatch")

	// check who was notified
	var actualNotifiedIDs []models.UserID
	err = db.SelectContext(ctx, &actualNotifiedIDs, `SELECT user_id FROM notifications_notification WHERE log_id = $1`, log.ID)
	require.NoError(t, err)

	expectedNotifiedIDs := make([]models.UserID, len(expectedNotified))
	for i := range expectedNotified {
		expectedNotifiedIDs[i] = expectedNotified[i].ID
	}

	assert.ElementsMatch(t, expectedNotifiedIDs, actualNotifiedIDs, "notified users mismatch")

	return log
}

func openTicket(ctx context.Context, db *sqlx.DB, openedBy *testdata.User, assignee *testdata.User) (*models.Ticket, *models.TicketEvent) {
	ticket := testdata.InsertOpenTicket(db, testdata.Org1, testdata.Cathy, testdata.Internal, testdata.SupportTopic, "", "Where my pants", "", assignee)
	modelTicket := ticket.Load(db)

	openedEvent := models.NewTicketOpenedEvent(modelTicket, openedBy.SafeID(), assignee.SafeID())
	models.InsertTicketEvents(ctx, db, []*models.TicketEvent{openedEvent})

	return modelTicket, openedEvent
}
