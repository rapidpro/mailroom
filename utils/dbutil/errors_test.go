package dbutil_test

import (
	"testing"

	"github.com/lib/pq"
	"github.com/nyaruka/mailroom/utils/dbutil"
	"github.com/sirupsen/logrus"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestIsUniqueViolation(t *testing.T) {
	var err error = &pq.Error{Code: pq.ErrorCode("23505")}

	assert.True(t, dbutil.IsUniqueViolation(err))
	assert.False(t, dbutil.IsUniqueViolation(errors.New("boom")))
}

func TestQueryError(t *testing.T) {
	var err error = &pq.Error{Code: pq.ErrorCode("22025"), Message: "unsupported Unicode escape sequence"}

	qerr := dbutil.NewQueryErrorf(err, "SELECT * FROM foo WHERE id = $1", []interface{}{234}, "error selecting foo %d", 234)
	assert.Error(t, qerr)
	assert.Equal(t, `error selecting foo 234: pq: unsupported Unicode escape sequence`, qerr.Error())

	// can unwrap to the original error
	var pqerr *pq.Error
	assert.True(t, errors.As(qerr, &pqerr))
	assert.Equal(t, err, pqerr)

	// can unwrap a wrapped error to find the first query error
	wrapped := errors.Wrap(errors.Wrap(qerr, "error doing this"), "error doing that")
	unwrapped := dbutil.AsQueryError(wrapped)
	assert.Equal(t, qerr, unwrapped)

	// nil if error was never a query error
	wrapped = errors.Wrap(errors.New("error doing this"), "error doing that")
	assert.Nil(t, dbutil.AsQueryError(wrapped))

	assert.Equal(t, logrus.Fields{"sql": "SELECT * FROM foo WHERE id = $1", "sql_args": []interface{}{234}}, unwrapped.Fields())
}
