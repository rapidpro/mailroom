package models

import (
	"context"
	"encoding/json"
	"time"

	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/gocommon/jsonx"
	"github.com/pkg/errors"
)

// ChannelLogID is our type for a channel log id
type ChannelLogID int64

type ChanneLogType string

const (
	ChannelLogTypeIVRStart    = "ivr_start"
	ChannelLogTypeIVRIncoming = "ivr_incoming"
	ChannelLogTypeIVRCallback = "ivr_callback"
	ChannelLogTypeIVRStatus   = "ivr_status"
	ChannelLogTypeIVRHangup   = "ivr_hangup"
)

// ChannelLog is the mailroom struct that represents channel logs
type ChannelLog struct {
	ID           ChannelLogID `db:"id"`
	ChannelID    ChannelID    `db:"channel_id"`
	ConnectionID ConnectionID `db:"connection_id"`

	Type      ChanneLogType   `db:"log_type"`
	HTTPLogs  json.RawMessage `db:"http_logs"`
	IsError   bool            `db:"is_error"`
	ElapsedMS int             `db:"elapsed_ms"`
	CreatedOn time.Time       `db:"created_on"`
}

const sqlInsertChannelLog = `
INSERT INTO channels_channellog( channel_id,  connection_id,  log_type,  http_logs,  is_error,  elapsed_ms,  created_on)
                         VALUES(:channel_id, :connection_id, :log_type, :http_logs, :is_error, :elapsed_ms, :created_on)
  RETURNING id`

// NewChannelLog creates a new channel log from the given HTTP trace
func NewChannelLog(channelID ChannelID, conn *ChannelConnection, logType ChanneLogType, trace *httpx.Trace) *ChannelLog {
	httpLog := httpx.NewLog(trace, 2048, 50000, nil)

	isError := false
	if trace.Response == nil || trace.Response.StatusCode/100 != 2 {
		isError = true
	}

	l := &ChannelLog{
		ChannelID: channelID,
		Type:      logType,
		HTTPLogs:  jsonx.MustMarshal([]*httpx.Log{httpLog}),
		IsError:   isError,
		ElapsedMS: httpLog.ElapsedMS,
		CreatedOn: time.Now(),
	}

	if conn != nil {
		l.ConnectionID = conn.ID()
	}

	return l
}

// InsertChannelLogs writes the given channel logs to the db
func InsertChannelLogs(ctx context.Context, db Queryer, logs []*ChannelLog) error {
	err := BulkQuery(ctx, "insert channel log", db, sqlInsertChannelLog, logs)
	return errors.Wrapf(err, "error inserting channel logs")
}
