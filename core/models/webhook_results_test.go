package models

import (
	"testing"
	"time"

	"github.com/nyaruka/mailroom/testsuite"
	"github.com/stretchr/testify/assert"
)

func TestWebhookResults(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()

	tcs := []struct {
		OrgID       OrgID
		ContactID   ContactID
		URL         string
		Request     string
		StatusCode  int
		Response    string
		Duration    time.Duration
		RequestTime int
	}{
		{Org1, CathyID, "http://foo.bar", "GET http://foo.bar", 200, "hello world", time.Millisecond * 1501, 1501},
		{Org1, BobID, "http://foo.bar", "GET http://foo.bar", 200, "hello world", time.Millisecond * 1502, 1502},
	}

	for _, tc := range tcs {
		r := NewWebhookResult(tc.OrgID, tc.ContactID, tc.URL, tc.Request, tc.StatusCode, tc.Response, tc.Duration, time.Now())
		err := InsertWebhookResults(ctx, db, []*WebhookResult{r})
		assert.NoError(t, err)
		assert.NotZero(t, r.ID())

		testsuite.AssertQueryCount(t, db, `
		SELECT count(*) FROM api_webhookresult WHERE org_id = $1 AND contact_id = $2 AND url = $3 AND request = $4 AND
		status_code = $5 AND response = $6 AND request_time = $7
		`, []interface{}{tc.OrgID, tc.ContactID, tc.URL, tc.Request, tc.StatusCode, tc.Response, tc.RequestTime}, 1)
	}
}
