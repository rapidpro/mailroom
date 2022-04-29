package models

import (
	"context"
	"database/sql/driver"
	"time"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/null"
)

// HTTPLogID is our type for HTTPLog ids
type HTTPLogID null.Int

// HTTPLogType is the type for the type of log this is
type HTTPLogType string

const (
	// LogTypeWebhookCalled is our type for when a flow calls a webhook
	LogTypeWebhookCalled = "webhook_called"

	// LogTypeIntentsSynced is our type for when we sync intents
	LogTypeIntentsSynced = "intents_synced"

	// LogTypeClassifierCalled is our type for when we call a classifier
	LogTypeClassifierCalled = "classifier_called"

	// LogTypeTicketerCalled is our type for when we call a ticketer
	LogTypeTicketerCalled = "ticketer_called"

	// LogTypeAirtimeTransferred is our type for when we make an airtime transfer
	LogTypeAirtimeTransferred = "airtime_transferred"
)

// HTTPLog is our type for a HTTPLog
type HTTPLog struct {
	ID                HTTPLogID         `db:"id"`
	OrgID             OrgID             `db:"org_id"`
	LogType           HTTPLogType       `db:"log_type"`
	URL               string            `db:"url"`
	StatusCode        int               `db:"status_code"`
	Request           string            `db:"request"`
	Response          null.String       `db:"response"`
	IsError           bool              `db:"is_error"`
	RequestTime       int               `db:"request_time"`
	NumRetries        int               `db:"num_retries"`
	CreatedOn         time.Time         `db:"created_on"`
	FlowID            FlowID            `db:"flow_id"`
	ClassifierID      ClassifierID      `db:"classifier_id"`
	TicketerID        TicketerID        `db:"ticketer_id"`
	AirtimeTransferID AirtimeTransferID `db:"airtime_transfer_id"`
}

func newHTTPLog(orgID OrgID, logType HTTPLogType, url string, statusCode int, request, response string, isError bool, elapsed time.Duration, retries int, createdOn time.Time) *HTTPLog {
	return &HTTPLog{
		OrgID:       orgID,
		LogType:     logType,
		URL:         url,
		StatusCode:  statusCode,
		Request:     request,
		Response:    null.String(response),
		IsError:     isError,
		RequestTime: int(elapsed / time.Millisecond),
		NumRetries:  retries,
		CreatedOn:   createdOn,
	}
}

// NewWebhookCalledLog creates a new HTTP log for an in-flow webhook call
func NewWebhookCalledLog(orgID OrgID, fid FlowID, url string, statusCode int, request, response string, isError bool, elapsed time.Duration, retries int, createdOn time.Time) *HTTPLog {
	h := newHTTPLog(orgID, LogTypeWebhookCalled, url, statusCode, request, response, isError, elapsed, retries, createdOn)
	h.FlowID = fid
	return h
}

// NewClassifierCalledLog creates a new HTTP log for a classifier call
func NewClassifierCalledLog(orgID OrgID, cid ClassifierID, url string, statusCode int, request, response string, isError bool, elapsed time.Duration, retries int, createdOn time.Time) *HTTPLog {
	h := newHTTPLog(orgID, LogTypeClassifierCalled, url, statusCode, request, response, isError, elapsed, retries, createdOn)
	h.ClassifierID = cid
	return h
}

// NewTicketerCalledLog creates a new HTTP log for a ticketer call
func NewTicketerCalledLog(orgID OrgID, tid TicketerID, url string, statusCode int, request, response string, isError bool, elapsed time.Duration, retries int, createdOn time.Time) *HTTPLog {
	h := newHTTPLog(orgID, LogTypeTicketerCalled, url, statusCode, request, response, isError, elapsed, retries, createdOn)
	h.TicketerID = tid
	return h
}

// NewAirtimeTransferredLog creates a new HTTP log for an airtime transfer
func NewAirtimeTransferredLog(orgID OrgID, url string, statusCode int, request, response string, isError bool, elapsed time.Duration, retries int, createdOn time.Time) *HTTPLog {
	return newHTTPLog(orgID, LogTypeAirtimeTransferred, url, statusCode, request, response, isError, elapsed, retries, createdOn)
}

// SetAirtimeTransferID called to set the transfer ID on a log after the transfer has been created
func (h *HTTPLog) SetAirtimeTransferID(tid AirtimeTransferID) {
	h.AirtimeTransferID = tid
}

const insertHTTPLogsSQL = `
INSERT INTO request_logs_httplog( log_type,  org_id,  url,  status_code,  flow_id,  classifier_id,  ticketer_id,  airtime_transfer_id,  request,  response,  is_error,  request_time,  num_retries,  created_on)
					      VALUES(:log_type, :org_id, :url, :status_code, :flow_id, :classifier_id, :ticketer_id, :airtime_transfer_id, :request, :response, :is_error, :request_time, :num_retries, :created_on)
RETURNING id
`

// InsertHTTPLogs inserts the passed in logs returning any errors encountered
func InsertHTTPLogs(ctx context.Context, tx Queryer, logs []*HTTPLog) error {
	return BulkQuery(ctx, "inserted http logs", tx, insertHTTPLogsSQL, logs)
}

// MarshalJSON marshals into JSON. 0 values will become null
func (i HTTPLogID) MarshalJSON() ([]byte, error) {
	return null.Int(i).MarshalJSON()
}

// UnmarshalJSON unmarshals from JSON. null values become 0
func (i *HTTPLogID) UnmarshalJSON(b []byte) error {
	return null.UnmarshalInt(b, (*null.Int)(i))
}

// Value returns the db value, null is returned for 0
func (i HTTPLogID) Value() (driver.Value, error) {
	return null.Int(i).Value()
}

// Scan scans from the db value. null values become 0
func (i *HTTPLogID) Scan(value interface{}) error {
	return null.ScanInt(value, (*null.Int)(i))
}

// HTTPLogger is a logger for HTTPLogs
type HTTPLogger struct {
	logs []*HTTPLog
}

// Ticketer creates a callback for engine HTTP logs which are associated with the given ticketer
func (h *HTTPLogger) Ticketer(t *Ticketer) flows.HTTPLogCallback {
	return func(l *flows.HTTPLog) {
		h.logs = append(h.logs, NewTicketerCalledLog(
			t.OrgID(),
			t.ID(),
			l.URL,
			l.StatusCode,
			l.Request,
			l.Response,
			l.Status != flows.CallStatusSuccess,
			time.Duration(l.ElapsedMS)*time.Millisecond,
			l.Retries,
			l.CreatedOn,
		))
	}
}

// Insert this logger's logs into the database
func (h *HTTPLogger) Insert(ctx context.Context, db Queryer) error {
	if len(h.logs) > 0 {
		return InsertHTTPLogs(ctx, db, h.logs)
	}
	return nil
}
