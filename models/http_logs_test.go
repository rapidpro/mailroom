package models

import (
	"testing"
	"time"

	"github.com/nyaruka/mailroom/testsuite"
	"github.com/stretchr/testify/assert"
)

func TestHTTPLogs(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()

	// insert a log
	log := NewClassifierCalledLog(Org1, WitID, "http://foo.bar", "GET /", "STATUS 200", false, time.Second, time.Now())
	err := InsertHTTPLogs(ctx, db, []*HTTPLog{log})
	assert.Nil(t, err)

	testsuite.AssertQueryCount(t, db,
		`SELECT count(*) from request_logs_httplog WHERE org_id = $1 AND classifier_id = $2 AND is_error = FALSE`,
		[]interface{}{Org1, WitID}, 1)

	// insert a log with nil response
	log = NewClassifierCalledLog(Org1, WitID, "http://foo.bar", "GET /", "", true, time.Second, time.Now())
	err = InsertHTTPLogs(ctx, db, []*HTTPLog{log})
	assert.Nil(t, err)

	testsuite.AssertQueryCount(t, db,
		`SELECT count(*) from request_logs_httplog WHERE org_id = $1 AND classifier_id = $2 AND is_error = TRUE AND response IS NULL`,
		[]interface{}{Org1, WitID}, 1)
}
