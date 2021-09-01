package models

import (
	"context"

	"github.com/nyaruka/mailroom/utils/dbutil"
	"github.com/pkg/errors"
)

// LogID is our type for log ids
type LogID int

type LogType string

const (
	LogTypeBroadcastStarted   LogType = "bcast:started"
	LogTypeBroadcastCompleted LogType = "bcast:completed"
	LogTypeChannelAlert       LogType = "channel:alert"
	LogTypeExportStarted      LogType = "export:started"
	LogTypeExportCompleted    LogType = "export:completed"
	LogTypeFlowStartStarted   LogType = "start:started"
	LogTypeFlowStartCompleted LogType = "start:completed"
	LogTypeImportStarted      LogType = "import:started"
	LogTypeImportCompleted    LogType = "import:completed"
	LogTypeTicketOpened       LogType = "ticket:opened"
	LogTypeTicketNewMsgs      LogType = "ticket:msgs"
	LogTypeTicketAssigned     LogType = "ticket:assigned"
	LogTypeTicketNote         LogType = "ticket:note"
)

type Log struct {
	ID          LogID   `db:"id"`
	OrgID       OrgID   `db:"org_id"`
	LogType     LogType `db:"log_type"`
	CreatedByID UserID  `db:"created_by_id"`

	BroadcastID   BroadcastID   `db:"broadcast_id"`
	FlowStartID   StartID       `db:"flow_start_id"`
	TicketID      TicketID      `db:"ticket_id"`
	TicketEventID TicketEventID `db:"ticket_event_id"`
}

type Notification struct {
	ID     LogID  `db:"id"`
	OrgID  OrgID  `db:"org_id"`
	UserID UserID `db:"user_id"`
	LogID  LogID  `db:"log_id"`
}

type notifyWhoFunc func(l *Log) ([]UserRole, []UserID)

/*
func notifyUser(id UserID) notifyWhoFunc {
	return func(l *Log) ([]UserRole, []UserID) {
		return nil, []UserID{id}
	}
}

func notifyRoles(roles ...UserRole) notifyWhoFunc {
	return func(l *Log) ([]UserRole, []UserID) {
		return roles, nil
	}
}
*/

// LogTicketsOpened logs the opening of new tickets and notifies all assignable users if tickets is not already assigned
func LogTicketsOpened(ctx context.Context, db Queryer, oa *OrgAssets, events []*TicketEvent) error {
	// create log for each ticket event and record which users are assigned
	logs := make([]*Log, len(events))
	assignees := make(map[*Log]UserID, len(events))

	for i, evt := range events {
		logType := LogTypeTicketOpened
		if evt.AssigneeID() != NilUserID {
			logType = LogTypeTicketAssigned
		}

		log := &Log{
			OrgID:         evt.OrgID(),
			LogType:       logType,
			CreatedByID:   evt.CreatedByID(),
			TicketID:      evt.TicketID(),
			TicketEventID: evt.ID(),
		}
		logs[i] = log
		assignees[log] = evt.AssigneeID()
	}

	return insertLogsAndNotifications(ctx, db, oa, logs, func(l *Log) ([]UserRole, []UserID) {
		// if this log is actually an assignment then only notify the assignee
		if l.LogType == LogTypeTicketAssigned {
			return nil, []UserID{assignees[l]}
		}

		// otherwise notify all possible assignees
		return []UserRole{UserRoleAdministrator, UserRoleEditor, UserRoleAgent}, nil
	})
}

// LogTicketsAssigned logs the assignment of tickets and notifies the assignees
func LogTicketsAssigned(ctx context.Context, db Queryer, oa *OrgAssets, events []*TicketEvent) error {
	// create log for each ticket event and record which users are assigned
	logs := make([]*Log, len(events))
	assignees := make(map[*Log]UserID, len(events))

	for i, evt := range events {
		log := &Log{
			OrgID:         evt.OrgID(),
			LogType:       LogTypeTicketAssigned,
			CreatedByID:   evt.CreatedByID(),
			TicketID:      evt.TicketID(),
			TicketEventID: evt.ID(),
		}
		logs[i] = log
		assignees[log] = evt.AssigneeID()
	}

	return insertLogsAndNotifications(ctx, db, oa, logs, func(l *Log) ([]UserRole, []UserID) {
		assigneeID := assignees[l]

		// if ticket has been assigned, notify assignee
		if assigneeID != NilUserID {
			return nil, []UserID{assigneeID}
		}

		// otherwise don't notify anyone if it's being unassigned
		return nil, nil
	})
}

const insertLogSQL = `
INSERT INTO notifications_log(org_id,  log_type, created_on,  created_by_id,  broadcast_id,  flow_start_id,  ticket_id,  ticket_event_id) 
                      VALUES(:org_id, :log_type,      NOW(), :created_by_id, :broadcast_id, :flow_start_id, :ticket_id, :ticket_event_id)
RETURNING id`

const insertNotificationSQL = `
INSERT INTO notifications_notification(org_id,  user_id,  log_id,  is_seen) 
                               VALUES(:org_id, :user_id, :log_id,  FALSE)`

func insertLogsAndNotifications(ctx context.Context, db Queryer, oa *OrgAssets, logs []*Log, notifyWho notifyWhoFunc) error {
	is := make([]interface{}, len(logs))
	for i := range logs {
		is[i] = logs[i]
	}

	err := dbutil.BulkQuery(ctx, db, insertLogSQL, is)
	if err != nil {
		return errors.Wrap(err, "error inserting logs")
	}

	var notifications []interface{}

	for _, log := range logs {
		notifyRoles, notifyUsers := notifyWho(log)

		for _, u := range oa.users {
			user := u.(*User)

			// don't create a notification for the user that did the logged thing
			if log.CreatedByID != user.ID() && userMatchesRoleOrID(user, notifyRoles, notifyUsers) {
				notifications = append(notifications, &Notification{OrgID: oa.OrgID(), UserID: user.ID(), LogID: log.ID})
			}
		}
	}

	err = dbutil.BulkQuery(ctx, db, insertNotificationSQL, notifications)

	return errors.Wrap(err, "error inserting notifications")
}

func userMatchesRoleOrID(user *User, roles []UserRole, ids []UserID) bool {
	for _, r := range roles {
		if user.Role() == r {
			return true
		}
	}
	for _, id := range ids {
		if user.ID() == id {
			return true
		}
	}
	return false
}
