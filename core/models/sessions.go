package models

import (
	"context"

	"github.com/lib/pq"
	"github.com/pkg/errors"
)

// InterruptContactSessions interrupts any waiting sessions for the given contacts
func InterruptContactSessions(ctx context.Context, tx Queryer, contactIDs []ContactID) error {
	return interruptContactSessions(ctx, tx, contactIDs, "")
}

// InterruptContactSessionsOfType interrupts any waiting sessions of the given type for the given contacts
func InterruptContactSessionsOfType(ctx context.Context, tx Queryer, contactIDs []ContactID, sessionType FlowType) error {
	return interruptContactSessions(ctx, tx, contactIDs, sessionType)
}

func interruptContactSessions(ctx context.Context, tx Queryer, contactIDs []ContactID, sessionType FlowType) error {
	if len(contactIDs) == 0 {
		return nil
	}

	sessionIDs := make([]SessionID, 0, len(contactIDs))
	sql := `SELECT id FROM flows_flowsession WHERE status = 'W' AND contact_id = ANY($1)`
	params := []interface{}{pq.Array(contactIDs)}

	if sessionType != "" {
		sql += ` AND session_type = $2;`
		params = append(params, sessionType)
	}

	err := tx.SelectContext(ctx, &sessionIDs, sql, params...)
	if err != nil {
		return errors.Wrapf(err, "error selecting waiting sessions for contacts")
	}

	err = ExitSessions(ctx, tx, sessionIDs, SessionStatusInterrupted)
	if err != nil {
		return errors.Wrapf(err, "error exiting sessions")
	}

	return nil
}

const activeSessionIDsForChannelsSQL = `
SELECT fs.id
  FROM flows_flowsession fs
  JOIN channels_channelconnection cc ON fs.connection_id = cc.id
 WHERE fs.status = 'W' AND cc.channel_id = ANY($1);
`

// InterruptContactSessions interrupts any waiting sessions with connections on the given channels
func InterruptChannelSessions(ctx context.Context, tx Queryer, channelIDs []ChannelID) error {
	if len(channelIDs) == 0 {
		return nil
	}

	sessionIDs := make([]SessionID, 0, len(channelIDs))

	err := tx.SelectContext(ctx, &sessionIDs, activeSessionIDsForChannelsSQL, pq.Array(channelIDs))
	if err != nil {
		return errors.Wrapf(err, "error selecting waiting sessions for channels")
	}

	err = ExitSessions(ctx, tx, sessionIDs, SessionStatusInterrupted)
	if err != nil {
		return errors.Wrapf(err, "error exiting sessions")
	}

	return nil
}
