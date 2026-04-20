package models_test

import (
	"context"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTicketNotifications(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetData)

	oa, err := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
	require.NoError(t, err)

	t0 := time.Now()

	// open unassigned tickets by a flow (i.e. no user)
	ticket1, openedEvent1 := openTicket(t, ctx, rt, nil, nil)
	ticket2, openedEvent2 := openTicket(t, ctx, rt, nil, nil)
	err = models.NotificationsFromTicketEvents(ctx, rt.DB, oa, map[*models.Ticket]*models.TicketEvent{ticket1: openedEvent1, ticket2: openedEvent2})
	require.NoError(t, err)

	// check that all assignable users are notified once
	assertNotifications(t, ctx, rt.DB, t0, map[*testdata.User][]models.NotificationType{
		testdata.Admin:  {models.NotificationTypeTicketsOpened},
		testdata.Editor: {models.NotificationTypeTicketsOpened},
		testdata.Agent:  {models.NotificationTypeTicketsOpened},
	})

	t1 := time.Now()

	// another ticket opened won't create new notifications
	ticket3, openedEvent3 := openTicket(t, ctx, rt, nil, nil)
	err = models.NotificationsFromTicketEvents(ctx, rt.DB, oa, map[*models.Ticket]*models.TicketEvent{ticket3: openedEvent3})
	require.NoError(t, err)

	assertNotifications(t, ctx, rt.DB, t1, map[*testdata.User][]models.NotificationType{})

	// mark all notifications as seen
	rt.DB.MustExec(`UPDATE notifications_notification SET is_seen = TRUE`)

	// open an unassigned ticket by a user
	ticket4, openedEvent4 := openTicket(t, ctx, rt, testdata.Editor, nil)
	err = models.NotificationsFromTicketEvents(ctx, rt.DB, oa, map[*models.Ticket]*models.TicketEvent{ticket4: openedEvent4})
	require.NoError(t, err)

	// check that all assignable users are notified except the user that opened the ticket
	assertNotifications(t, ctx, rt.DB, t1, map[*testdata.User][]models.NotificationType{
		testdata.Admin: {models.NotificationTypeTicketsOpened},
		testdata.Agent: {models.NotificationTypeTicketsOpened},
	})

	t2 := time.Now()
	rt.DB.MustExec(`UPDATE notifications_notification SET is_seen = TRUE`)

	// open an already assigned ticket
	ticket5, openedEvent5 := openTicket(t, ctx, rt, nil, testdata.Agent)
	err = models.NotificationsFromTicketEvents(ctx, rt.DB, oa, map[*models.Ticket]*models.TicketEvent{ticket5: openedEvent5})
	require.NoError(t, err)

	// check that the assigned user gets a ticket activity notification
	assertNotifications(t, ctx, rt.DB, t2, map[*testdata.User][]models.NotificationType{
		testdata.Agent: {models.NotificationTypeTicketsActivity},
	})

	t3 := time.Now()

	// however if a user opens a ticket which is assigned to themselves, no notification
	ticket6, openedEvent6 := openTicket(t, ctx, rt, testdata.Admin, testdata.Admin)
	err = models.NotificationsFromTicketEvents(ctx, rt.DB, oa, map[*models.Ticket]*models.TicketEvent{ticket6: openedEvent6})
	require.NoError(t, err)

	// check that the assigned user gets a ticket activity notification
	assertNotifications(t, ctx, rt.DB, t3, map[*testdata.User][]models.NotificationType{})

	t4 := time.Now()
	rt.DB.MustExec(`UPDATE notifications_notification SET is_seen = TRUE`)

	// now have a user assign existing tickets to another user
	_, err = models.TicketsAssign(ctx, rt.DB, oa, testdata.Admin.ID, []*models.Ticket{ticket1, ticket2}, testdata.Agent.ID)
	require.NoError(t, err)

	// check that the assigned user gets a ticket activity notification
	assertNotifications(t, ctx, rt.DB, t4, map[*testdata.User][]models.NotificationType{
		testdata.Agent: {models.NotificationTypeTicketsActivity},
	})

	t5 := time.Now()
	rt.DB.MustExec(`UPDATE notifications_notification SET is_seen = TRUE`)

	// and finally a user assigning a ticket to themselves
	_, err = models.TicketsAssign(ctx, rt.DB, oa, testdata.Editor.ID, []*models.Ticket{ticket3}, testdata.Editor.ID)
	require.NoError(t, err)

	// no notifications for self-assignment
	assertNotifications(t, ctx, rt.DB, t5, map[*testdata.User][]models.NotificationType{})
}

func TestImportNotifications(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetData)

	importID := testdata.InsertContactImport(rt, testdata.Org1, testdata.Editor)
	imp, err := models.LoadContactImport(ctx, rt.DB, importID)
	require.NoError(t, err)

	err = imp.MarkFinished(ctx, rt.DB, models.ContactImportStatusComplete)
	require.NoError(t, err)

	t0 := time.Now()

	err = models.NotifyImportFinished(ctx, rt.DB, imp)
	require.NoError(t, err)

	assertNotifications(t, ctx, rt.DB, t0, map[*testdata.User][]models.NotificationType{
		testdata.Editor: {models.NotificationTypeImportFinished},
	})
}

func TestIncidentNotifications(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetData)

	oa, err := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
	require.NoError(t, err)

	t0 := time.Now()

	_, err = models.IncidentWebhooksUnhealthy(ctx, rt.DB, rt.RP, oa, nil)
	require.NoError(t, err)

	assertNotifications(t, ctx, rt.DB, t0, map[*testdata.User][]models.NotificationType{
		testdata.Admin: {models.NotificationTypeIncidentStarted},
	})
}

func assertNotifications(t *testing.T, ctx context.Context, db *sqlx.DB, after time.Time, expected map[*testdata.User][]models.NotificationType) {
	// check last log
	var notifications []*models.Notification
	err := db.SelectContext(ctx, &notifications, `SELECT id, org_id, notification_type, scope, user_id, is_seen, created_on FROM notifications_notification WHERE created_on > $1 ORDER BY id`, after)
	require.NoError(t, err)

	expectedByID := map[models.UserID][]models.NotificationType{}
	for user, notificationTypes := range expected {
		expectedByID[user.ID] = notificationTypes
	}

	actual := map[models.UserID][]models.NotificationType{}
	for _, notification := range notifications {
		actual[notification.UserID] = append(actual[notification.UserID], notification.Type)
	}

	assert.Equal(t, expectedByID, actual)
}

func openTicket(t *testing.T, ctx context.Context, rt *runtime.Runtime, openedBy *testdata.User, assignee *testdata.User) (*models.Ticket, *models.TicketEvent) {
	ticket := testdata.InsertOpenTicket(rt, testdata.Org1, testdata.Cathy, testdata.SupportTopic, "Where my pants", time.Now(), assignee)
	modelTicket := ticket.Load(rt)

	openedEvent := models.NewTicketOpenedEvent(modelTicket, openedBy.SafeID(), assignee.SafeID())
	err := models.InsertTicketEvents(ctx, rt.DB, []*models.TicketEvent{openedEvent})
	require.NoError(t, err)

	return modelTicket, openedEvent
}
