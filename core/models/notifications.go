package models

import (
	"context"
	"fmt"
	"time"

	"github.com/nyaruka/mailroom/utils/dbutil"
	"github.com/pkg/errors"
)

// NotificationID is our type for notification ids
type NotificationID int

type NotificationType string

const (
	NotificationTypeChannelAlert    NotificationType = "channel:alert"
	NotificationTypeExportFinished  NotificationType = "export:finished"
	NotificationTypeImportFinished  NotificationType = "import:finished"
	NotificationTypeTicketsOpened   NotificationType = "tickets:opened"
	NotificationTypeTicketsActivity NotificationType = "tickets:activity"
)

type EmailStatus string

const (
	EmailStatusPending = "P"
	EmailStatusSent    = "S"
	EmailStatusNone    = "N"
)

type Notification struct {
	ID          NotificationID   `db:"id"`
	OrgID       OrgID            `db:"org_id"`
	Type        NotificationType `db:"notification_type"`
	Scope       string           `db:"scope"`
	UserID      UserID           `db:"user_id"`
	IsSeen      bool             `db:"is_seen"`
	EmailStatus EmailStatus      `db:"email_status"`
	CreatedOn   time.Time        `db:"created_on"`

	ChannelID       ChannelID       `db:"channel_id"`
	ContactImportID ContactImportID `db:"contact_import_id"`
}

// NotifyImportFinished logs the the finishing of a contact import
func NotifyImportFinished(ctx context.Context, db Queryer, imp *ContactImport) error {
	n := &Notification{
		OrgID:           imp.OrgID,
		Type:            NotificationTypeImportFinished,
		Scope:           fmt.Sprintf("contact:%d", imp.ID),
		UserID:          imp.CreatedByID,
		ContactImportID: imp.ID,
	}

	return insertNotifications(ctx, db, []*Notification{n})
}

var ticketAssignableToles = []UserRole{UserRoleAdministrator, UserRoleEditor, UserRoleAgent}

// NotificationsFromTicketEvents logs the opening of new tickets and notifies all assignable users if tickets is not already assigned
func NotificationsFromTicketEvents(ctx context.Context, db Queryer, oa *OrgAssets, events map[*Ticket]*TicketEvent) error {
	notifyTicketsOpened := make(map[UserID]bool)
	notifyTicketsActivity := make(map[UserID]bool)

	for ticket, evt := range events {
		switch evt.EventType() {
		case TicketEventTypeOpened:
			// if ticket is unassigned notify all possible assignees
			if evt.AssigneeID() == NilUserID {
				for _, u := range oa.users {
					user := u.(*User)

					if hasAnyRole(user, ticketAssignableToles) && evt.CreatedByID() != user.ID() {
						notifyTicketsOpened[user.ID()] = true
					}
				}
			} else if evt.AssigneeID() != evt.CreatedByID() {
				notifyTicketsActivity[evt.AssigneeID()] = true
			}
		case TicketEventTypeAssigned:
			// notify new ticket assignee if they didn't self-assign
			if evt.AssigneeID() != NilUserID && evt.AssigneeID() != evt.CreatedByID() {
				notifyTicketsActivity[evt.AssigneeID()] = true
			}
		case TicketEventTypeNoteAdded:
			// notify ticket assignee if they didn't add note themselves
			if ticket.AssigneeID() != NilUserID && ticket.AssigneeID() != evt.CreatedByID() {
				notifyTicketsActivity[ticket.AssigneeID()] = true
			}
		}
	}

	notifications := make([]*Notification, 0, len(events))

	for userID := range notifyTicketsOpened {
		notifications = append(notifications, &Notification{
			OrgID:  oa.OrgID(),
			Type:   NotificationTypeTicketsOpened,
			Scope:  "",
			UserID: userID,
		})
	}

	for userID := range notifyTicketsActivity {
		notifications = append(notifications, &Notification{
			OrgID:  oa.OrgID(),
			Type:   NotificationTypeTicketsActivity,
			Scope:  "",
			UserID: userID,
		})
	}

	return insertNotifications(ctx, db, notifications)
}

const insertNotificationSQL = `
INSERT INTO notifications_notification(org_id,  notification_type,  scope,  user_id, is_seen, email_status, created_on,  channel_id,  contact_import_id) 
                               VALUES(:org_id, :notification_type, :scope, :user_id,   FALSE,          'N',      NOW(), :channel_id, :contact_import_id) 
							   ON CONFLICT DO NOTHING`

func insertNotifications(ctx context.Context, db Queryer, notifications []*Notification) error {
	is := make([]interface{}, len(notifications))
	for i := range notifications {
		is[i] = notifications[i]
	}

	err := dbutil.BulkQuery(ctx, db, insertNotificationSQL, is)
	return errors.Wrap(err, "error inserting notifications")
}

func hasAnyRole(user *User, roles []UserRole) bool {
	for _, r := range roles {
		if user.Role() == r {
			return true
		}
	}
	return false
}
