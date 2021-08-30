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
	LogTypeTicketAssignment   LogType = "ticket:assign"
	LogTypeTicketNote         LogType = "ticket:note"
)

type Log struct {
	ID          LogID   `db:"id"`
	OrgID       OrgID   `db:"org_id"`
	LogType     LogType `db:"log_type"`
	CreatedByID UserID  `db:"created_by_id"`

	BroadcastID BroadcastID `db:"broadcast_id"`
	FlowStartID StartID     `db:"flow_start_id"`
	TicketID    TicketID    `db:"ticket_id"`
}

type Notification struct {
	ID     LogID  `db:"id"`
	OrgID  OrgID  `db:"org_id"`
	UserID UserID `db:"user_id"`
	LogID  LogID  `db:"log_id"`
}

func LogTicketOpened(ctx context.Context, db Queryer, oa *OrgAssets, ticket *Ticket) error {
	log := &Log{OrgID: ticket.OrgID(), LogType: LogTypeTicketOpened, TicketID: ticket.ID()}
	return logAndNotify(ctx, db, oa, log, []UserRole{UserRoleAdministrator, UserRoleEditor, UserRoleAgent}, false)
}

const insertLogSQL = `
INSERT INTO notifications_log(org_id,  log_type, created_on,  created_by_id,  broadcast_id,  flow_start_id,  ticket_id) 
                      VALUES(:org_id, :log_type,      NOW(), :created_by_id, :broadcast_id, :flow_start_id, :ticket_id)
RETURNING id`

const insertNotificationSQL = `
INSERT INTO notifications_notification(org_id,  user_id,  log_id,  is_seen) 
                               VALUES(:org_id, :user_id, :log_id,  FALSE)`

func logAndNotify(ctx context.Context, db Queryer, oa *OrgAssets, log *Log, notifyRoles []UserRole, notifyUser bool) error {
	// TODO use a more efficient simple Exec
	err := dbutil.BulkQuery(ctx, db, insertLogSQL, []interface{}{log})
	if err != nil {
		return errors.Wrapf(err, "error inserting %s log", log.LogType)
	}

	notifyUsers := getUsersToNotify(oa, notifyRoles, notifyUser, log.CreatedByID)

	var notifications []interface{}
	for _, user := range notifyUsers {
		notifications = append(notifications, &Notification{OrgID: oa.OrgID(), UserID: user.ID(), LogID: log.ID})
	}

	err = dbutil.BulkQuery(ctx, db, insertNotificationSQL, notifications)

	return errors.Wrap(err, "error inserting notifications")
}

func getUsersToNotify(oa *OrgAssets, notifyRoles []UserRole, notifyUser bool, logUserID UserID) []*User {
	var users []*User

	for _, u := range oa.users {
		user := u.(*User)

		if !notifyUser && logUserID == user.ID() {
			continue
		}

		for _, r := range notifyRoles {
			if user.Role() == r {
				users = append(users, user)
				break
			}
		}
	}

	return users
}
