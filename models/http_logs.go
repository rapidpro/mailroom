package models

import (
	"context"
	"database/sql/driver"
	"time"

	"github.com/nyaruka/null"
)

// HTTPLogID is our type for HTTPLog ids
type HTTPLogID null.Int

// HTTPLogType is the type for the type of log this is
type HTTPLogType string

const (
	// LogTypeIntentsSynced is our type for when we sync intents
	LogTypeIntentsSynced = "intents_synced"

	// LogTypeClassifierCalled is our type for when we call a classifier
	LogTypeClassifierCalled = "classifier_called"
)

// HTTPLog is our type for a HTTPLog
type HTTPLog struct {
	h struct {
		ID                HTTPLogID         `db:"id"`
		LogType           HTTPLogType       `db:"log_type"`
		ClassifierID      ClassifierID      `db:"classifier_id"`
		AirtimeTransferID AirtimeTransferID `db:"airtime_transfer_id"`
		URL               string            `db:"url"`
		Request           string            `db:"request"`
		Response          null.String       `db:"response"`
		IsError           bool              `db:"is_error"`
		RequestTime       int               `db:"request_time"`
		CreatedOn         time.Time         `db:"created_on"`
		OrgID             OrgID             `db:"org_id"`
	}
}

// NewHTTPLog creates a new HTTPLog
func NewHTTPLog(orgID OrgID, url string, request string, response string, isError bool, elapsed time.Duration, createdOn time.Time) *HTTPLog {
	h := &HTTPLog{}
	h.h.LogType = LogTypeClassifierCalled
	h.h.OrgID = orgID
	h.h.URL = url
	h.h.Request = request
	h.h.Response = null.String(response)
	h.h.IsError = isError
	h.h.RequestTime = int(elapsed / time.Millisecond)
	h.h.CreatedOn = createdOn
	return h
}

// NewClassifierCalledLog creates a new HTTPLog for a classifier call
func NewClassifierCalledLog(orgID OrgID, cid ClassifierID, url string, request string, response string, isError bool, elapsed time.Duration, createdOn time.Time) *HTTPLog {
	h := &HTTPLog{}
	h.h.LogType = LogTypeClassifierCalled
	h.h.OrgID = orgID
	h.h.ClassifierID = cid
	h.h.URL = url
	h.h.Request = request
	h.h.Response = null.String(response)
	h.h.IsError = isError
	h.h.RequestTime = int(elapsed / time.Millisecond)
	h.h.CreatedOn = createdOn
	return h
}

// SetAirtimeTransferID sets the transfer ID on a log
func (h *HTTPLog) SetAirtimeTransferID(transferID AirtimeTransferID) {
	h.h.AirtimeTransferID = transferID
}

const insertHTTPLogsSQL = `
INSERT INTO request_logs_httplog( log_type,  org_id,  classifier_id,  airtime_transfer_id,  url,  request,  response,  is_error,  request_time,  created_on)
					      VALUES(:log_type, :org_id, :classifier_id, :airtime_transfer_id, :url, :request, :response, :is_error, :request_time, :created_on)
RETURNING id
`

// InsertHTTPLogs inserts the passed in logs returning any errors encountered
func InsertHTTPLogs(ctx context.Context, tx Queryer, logs []*HTTPLog) error {
	if len(logs) == 0 {
		return nil
	}

	ls := make([]interface{}, len(logs))
	for i := range logs {
		ls[i] = &logs[i].h
	}

	return BulkSQL(ctx, "inserted http logs", tx, insertHTTPLogsSQL, ls)
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
