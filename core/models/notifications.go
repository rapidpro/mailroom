package models

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/nyaruka/gocommon/dbutil"
	"github.com/pkg/errors"
)

// NotificationID is our type for notification ids
type NotificationID int64

type NotificationType string

const (
	NotificationTypeExportFinished  NotificationType = "export:finished"
	NotificationTypeImportFinished  NotificationType = "import:finished"
	NotificationTypeIncidentStarted NotificationType = "incident:started"
	NotificationTypeTicketsOpened   NotificationType = "tickets:opened"
	NotificationTypeTicketsActivity NotificationType = "tickets:activity"
)

type EmailStatus string

const (
	EmailStatusPending EmailStatus = "P"
	EmailStatusSent    EmailStatus = "S"
	EmailStatusNone    EmailStatus = "N"
)

const (
	MediumUI    = "U"
	MediumEmail = "E"
)

type Notification struct {
	ID          NotificationID   `db:"id"`
	OrgID       OrgID            `db:"org_id"`
	Type        NotificationType `db:"notification_type"`
	Scope       string           `db:"scope"`
	UserID      UserID           `db:"user_id"`
	Medium      string           `db:"medium"`
	IsSeen      bool             `db:"is_seen"`
	EmailStatus EmailStatus      `db:"email_status"`
	CreatedOn   time.Time        `db:"created_on"`

	ContactImportID ContactImportID `db:"contact_import_id"`
	IncidentID      IncidentID      `db:"incident_id"`
}

// NotifyImportFinished notifies the user who created an import that it has finished
func NotifyImportFinished(ctx context.Context, db DBorTx, imp *ContactImport) error {
	n := &Notification{
		OrgID:           imp.OrgID,
		Type:            NotificationTypeImportFinished,
		Scope:           fmt.Sprintf("contact:%d", imp.ID),
		UserID:          imp.CreatedByID,
		Medium:          MediumUI,
		EmailStatus:     EmailStatusNone,
		ContactImportID: imp.ID,
	}

	return insertNotifications(ctx, db, []*Notification{n})
}

// NotifyIncidentStarted notifies administrators that an incident has started
func NotifyIncidentStarted(ctx context.Context, db DBorTx, oa *OrgAssets, incident *Incident) error {
	admins := usersWithRoles(oa, []UserRole{UserRoleAdministrator})
	notifications := make([]*Notification, len(admins))

	for i, admin := range admins {
		notifications[i] = &Notification{
			OrgID:       incident.OrgID,
			Type:        NotificationTypeIncidentStarted,
			Scope:       strconv.Itoa(int(incident.ID)),
			UserID:      admin.ID(),
			Medium:      MediumUI,
			EmailStatus: EmailStatusNone,
			IncidentID:  incident.ID,
		}
	}

	return insertNotifications(ctx, db, notifications)
}

var ticketAssignableToles = []UserRole{UserRoleAdministrator, UserRoleEditor, UserRoleAgent}

// NotificationsFromTicketEvents logs the opening of new tickets and notifies all assignable users if tickets is not already assigned
func NotificationsFromTicketEvents(ctx context.Context, db DBorTx, oa *OrgAssets, events map[*Ticket]*TicketEvent) error {
	notifyTicketsOpened := make(map[UserID]bool)
	notifyTicketsActivity := make(map[UserID]bool)

	assignableUsers := usersWithRoles(oa, ticketAssignableToles)

	for ticket, evt := range events {
		switch evt.EventType() {
		case TicketEventTypeOpened:
			// if ticket is unassigned notify all possible assignees
			if evt.AssigneeID() == NilUserID {
				for _, user := range assignableUsers {
					if evt.CreatedByID() != user.ID() {
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
			OrgID:       oa.OrgID(),
			Type:        NotificationTypeTicketsOpened,
			Scope:       "",
			UserID:      userID,
			Medium:      MediumUI,
			EmailStatus: EmailStatusNone,
		})
	}

	for userID := range notifyTicketsActivity {
		notifications = append(notifications, &Notification{
			OrgID:       oa.OrgID(),
			Type:        NotificationTypeTicketsActivity,
			Scope:       "",
			UserID:      userID,
			Medium:      MediumUI,
			EmailStatus: EmailStatusNone,
		})
	}

	return insertNotifications(ctx, db, notifications)
}

const insertNotificationSQL = `
INSERT INTO notifications_notification(org_id,  notification_type,  scope,  user_id,  medium, is_seen,  email_status, created_on,  contact_import_id,  incident_id) 
                               VALUES(:org_id, :notification_type, :scope, :user_id, :medium,   FALSE, :email_status,      NOW(), :contact_import_id, :incident_id) 
							   ON CONFLICT DO NOTHING`

func insertNotifications(ctx context.Context, db DBorTx, notifications []*Notification) error {
	err := dbutil.BulkQuery(ctx, db, insertNotificationSQL, notifications)
	return errors.Wrap(err, "error inserting notifications")
}

func usersWithRoles(oa *OrgAssets, roles []UserRole) []*User {
	users := make([]*User, 0, 5)
	for _, u := range oa.users {
		user := u.(*User)
		if hasAnyRole(user, roles) {
			users = append(users, user)
		}
	}
	return users
}

func hasAnyRole(user *User, roles []UserRole) bool {
	for _, r := range roles {
		if user.Role() == r {
			return true
		}
	}
	return false
}
