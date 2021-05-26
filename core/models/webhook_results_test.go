package models_test

import (
	"testing"
	"time"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"

	"github.com/stretchr/testify/assert"
)

func TestWebhookResults(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()

	tcs := []struct {
		OrgID       models.OrgID
		ContactID   models.ContactID
		URL         string
		Request     string
		StatusCode  int
		Response    string
		Duration    time.Duration
		RequestTime int
	}{
		{testdata.Org1.ID, testdata.Cathy.ID, "http://foo.bar", "GET http://foo.bar", 200, "hello world", time.Millisecond * 1501, 1501},
		{testdata.Org1.ID, testdata.Bob.ID, "http://foo.bar", "GET http://foo.bar", 200, "hello world", time.Millisecond * 1502, 1502},
	}

	for _, tc := range tcs {
		r := models.NewWebhookResult(tc.OrgID, tc.ContactID, tc.URL, tc.Request, tc.StatusCode, tc.Response, tc.Duration, time.Now())
		err := models.InsertWebhookResults(ctx, db, []*models.WebhookResult{r})
		assert.NoError(t, err)
		assert.NotZero(t, r.ID())

		testsuite.AssertQueryCount(t, db, `
		SELECT count(*) FROM api_webhookresult WHERE org_id = $1 AND contact_id = $2 AND url = $3 AND request = $4 AND
		status_code = $5 AND response = $6 AND request_time = $7
		`, []interface{}{tc.OrgID, tc.ContactID, tc.URL, tc.Request, tc.StatusCode, tc.Response, tc.RequestTime}, 1)
	}
}
