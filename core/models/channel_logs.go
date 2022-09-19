package models

import (
	"context"
	"encoding/json"
	"time"

	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/gocommon/stringsx"
	"github.com/nyaruka/gocommon/uuids"
	"github.com/pkg/errors"
)

// ChannelLogID is our type for a channel log id
type ChannelLogID int64

// ChannelLogUUID is our type for a channel log UUID
type ChannelLogUUID uuids.UUID

type ChannelLogType string

const (
	ChannelLogTypeIVRStart    = "ivr_start"
	ChannelLogTypeIVRIncoming = "ivr_incoming"
	ChannelLogTypeIVRCallback = "ivr_callback"
	ChannelLogTypeIVRStatus   = "ivr_status"
	ChannelLogTypeIVRHangup   = "ivr_hangup"
)

type ChannelError struct {
	Message string `json:"message"`
	Code    string `json:"code"`
}

func NewChannelError(message, code string) ChannelError {
	return ChannelError{Message: message, Code: code}
}

// ChannelLog stores the HTTP traces and errors generated by an interaction with a channel.
type ChannelLog struct {
	uuid      ChannelLogUUID
	type_     ChannelLogType
	channel   *Channel
	call      *Call
	httpLogs  []*httpx.Log
	errors    []ChannelError
	createdOn time.Time
	elapsed   time.Duration

	recorder *httpx.Recorder
	redactor stringsx.Redactor
}

// NewChannelLog creates a new channel log with the given type and channel
func NewChannelLog(t ChannelLogType, ch *Channel, redactVals []string) *ChannelLog {
	return newChannelLog(t, ch, nil, redactVals)
}

// NewChannelLogForIncoming creates a new channel log for an incoming request
func NewChannelLogForIncoming(t ChannelLogType, ch *Channel, r *httpx.Recorder, redactVals []string) *ChannelLog {
	return newChannelLog(t, ch, r, redactVals)
}

func newChannelLog(t ChannelLogType, ch *Channel, r *httpx.Recorder, redactVals []string) *ChannelLog {
	return &ChannelLog{
		uuid:      ChannelLogUUID(uuids.New()),
		type_:     t,
		channel:   ch,
		createdOn: dates.Now(),

		recorder: r,
		redactor: stringsx.NewRedactor("**********", redactVals...),
	}
}

func (l *ChannelLog) UUID() ChannelLogUUID { return l.uuid }

func (l *ChannelLog) SetCall(c *Call) {
	l.call = c
}

func (l *ChannelLog) HTTP(t *httpx.Trace) {
	l.httpLogs = append(l.httpLogs, l.traceToLog(t))
}

func (l *ChannelLog) Error(err error) {
	l.errors = append(l.errors, NewChannelError(err.Error(), ""))
}

func (l *ChannelLog) End() {
	if l.recorder != nil {
		// prepend so it's the first HTTP request in the log
		l.httpLogs = append([]*httpx.Log{l.traceToLog(l.recorder.Trace)}, l.httpLogs...)
	}

	l.elapsed = time.Since(l.createdOn)
}

func (l *ChannelLog) traceToLog(t *httpx.Trace) *httpx.Log {
	return httpx.NewLog(t, 2048, 50000, l.redactor)
}

const sqlInsertChannelLog = `
INSERT INTO channels_channellog( uuid,  channel_id,  connection_id,  log_type,  http_logs,  errors,  is_error,  elapsed_ms,  created_on)
                         VALUES(:uuid, :channel_id, :connection_id, :log_type, :http_logs, :errors, :is_error, :elapsed_ms, :created_on)
  RETURNING id`

type dbChannelLog struct {
	ID           ChannelLogID    `db:"id"`
	UUID         ChannelLogUUID  `db:"uuid"`
	ChannelID    ChannelID       `db:"channel_id"`
	ConnectionID CallID          `db:"connection_id"`
	Type         ChannelLogType  `db:"log_type"`
	HTTPLogs     json.RawMessage `db:"http_logs"`
	Errors       json.RawMessage `db:"errors"`
	IsError      bool            `db:"is_error"`
	ElapsedMS    int             `db:"elapsed_ms"`
	CreatedOn    time.Time       `db:"created_on"`
}

// InsertChannelLogs writes the given channel logs to the db
func InsertChannelLogs(ctx context.Context, db Queryer, logs []*ChannelLog) error {
	vs := make([]*dbChannelLog, len(logs))
	for i, l := range logs {
		// if we have an error or a non 2XX/3XX http response then this log is marked as an error
		isError := len(l.errors) > 0
		if !isError {
			for _, l := range l.httpLogs {
				if l.StatusCode < 200 || l.StatusCode >= 400 {
					isError = true
					break
				}
			}
		}

		v := &dbChannelLog{
			UUID:      ChannelLogUUID(uuids.New()),
			ChannelID: l.channel.ID(),
			Type:      l.type_,
			HTTPLogs:  jsonx.MustMarshal(l.httpLogs),
			Errors:    jsonx.MustMarshal(l.errors),
			IsError:   isError,
			CreatedOn: time.Now(),
			ElapsedMS: int(l.elapsed / time.Millisecond),
		}
		if l.call != nil {
			v.ConnectionID = l.call.ID()
		}
		vs[i] = v
	}

	err := BulkQuery(ctx, "insert channel log", db, sqlInsertChannelLog, vs)
	return errors.Wrapf(err, "error inserting channel logs")
}
