package models

import (
	"context"

	"github.com/lib/pq"
	"github.com/pkg/errors"
)

// InterruptSessionsForContacts interrupts any waiting sessions for the given contacts
func InterruptSessionsForContacts(ctx context.Context, tx Queryer, contactIDs []ContactID) error {
	return interruptSessionsForContacts(ctx, tx, contactIDs, "")
}

// InterruptSessionsOfTypeForContacts interrupts any waiting sessions of the given type for the given contacts
func InterruptSessionsOfTypeForContacts(ctx context.Context, tx Queryer, contactIDs []ContactID, sessionType FlowType) error {
	return interruptSessionsForContacts(ctx, tx, contactIDs, sessionType)
}

func interruptSessionsForContacts(ctx context.Context, tx Queryer, contactIDs []ContactID, sessionType FlowType) error {
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

	return errors.Wrapf(ExitSessions(ctx, tx, sessionIDs, SessionStatusInterrupted), "error exiting sessions")
}

const sqlWaitingSessionIDsForChannels = `
SELECT fs.id
  FROM flows_flowsession fs
  JOIN channels_channelconnection cc ON fs.connection_id = cc.id
 WHERE fs.status = 'W' AND cc.channel_id = ANY($1);`

// InterruptSessionsForChannels interrupts any waiting sessions with connections on the given channels
func InterruptSessionsForChannels(ctx context.Context, tx Queryer, channelIDs []ChannelID) error {
	if len(channelIDs) == 0 {
		return nil
	}

	sessionIDs := make([]SessionID, 0, len(channelIDs))

	err := tx.SelectContext(ctx, &sessionIDs, sqlWaitingSessionIDsForChannels, pq.Array(channelIDs))
	if err != nil {
		return errors.Wrapf(err, "error selecting waiting sessions for channels")
	}

	return errors.Wrapf(ExitSessions(ctx, tx, sessionIDs, SessionStatusInterrupted), "error exiting sessions")
}

const sqlWaitingSessionIDsForFlows = `
SELECT id
  FROM flows_flowsession
 WHERE status = 'W' AND current_flow_id = ANY($1);`

// InterruptSessionsForFlows interrupts any waiting sessions currently in the given flows
func InterruptSessionsForFlows(ctx context.Context, tx Queryer, flowIDs []FlowID) error {
	if len(flowIDs) == 0 {
		return nil
	}

	sessionIDs := make([]SessionID, 0, len(flowIDs))

	err := tx.SelectContext(ctx, &sessionIDs, sqlWaitingSessionIDsForFlows, pq.Array(flowIDs))
	if err != nil {
		return errors.Wrapf(err, "error selecting waiting sessions for flows")
	}

	return errors.Wrapf(ExitSessions(ctx, tx, sessionIDs, SessionStatusInterrupted), "error exiting sessions")
}
