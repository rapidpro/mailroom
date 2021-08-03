package dbutil

import (
	"errors"
	"fmt"

	"github.com/lib/pq"
	"github.com/sirupsen/logrus"
)

// IsUniqueViolation returns true if the given error is a violation of unique constraint
func IsUniqueViolation(err error) bool {
	if pqErr, ok := err.(*pq.Error); ok {
		return pqErr.Code.Name() == "unique_violation"
	}
	return false
}

// QueryError is an error type for failed SQL queries
type QueryError struct {
	cause   error
	message string
	sql     string
	sqlArgs []interface{}
}

func (e *QueryError) Error() string {
	return e.message + ": " + e.cause.Error()
}

func (e *QueryError) Unwrap() error {
	return e.cause
}

func (e *QueryError) Fields() logrus.Fields {
	return logrus.Fields{
		"sql":      fmt.Sprintf("%.1000s", e.sql),
		"sql_args": e.sqlArgs,
	}
}

func NewQueryErrorf(cause error, sql string, sqlArgs []interface{}, message string, msgArgs ...interface{}) error {
	return &QueryError{
		cause:   cause,
		message: fmt.Sprintf(message, msgArgs...),
		sql:     sql,
		sqlArgs: sqlArgs,
	}
}

func AsQueryError(err error) *QueryError {
	var qerr *QueryError
	errors.As(err, &qerr)
	return qerr
}
