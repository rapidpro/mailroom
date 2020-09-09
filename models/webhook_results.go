package models

import (
	"context"
	"time"
)

type ResultID int64

// WebhookResult represents a result of a webhook or resthook call
type WebhookResult struct {
	r struct {
		ID          ResultID  `db:"id"`
		URL         string    `db:"url"`
		Request     string    `db:"request"`
		StatusCode  int       `db:"status_code"`
		Response    string    `db:"response"`
		RequestTime int       `db:"request_time"`
		ContactID   ContactID `db:"contact_id"`
		OrgID       OrgID     `db:"org_id"`
		CreatedOn   time.Time `db:"created_on"`
	}
}

func (r *WebhookResult) ID() ResultID { return r.r.ID }

// NewWebhookResult creates a new webhook result with the passed in parameters
func NewWebhookResult(
	orgID OrgID, contactID ContactID,
	url string, request string, statusCode int, response string,
	elapsed time.Duration, createdOn time.Time) *WebhookResult {
	result := &WebhookResult{}
	r := &result.r

	r.OrgID = orgID
	r.ContactID = contactID
	r.URL = url
	r.Request = request
	r.StatusCode = statusCode
	r.Response = response
	r.RequestTime = int(elapsed / time.Millisecond)
	r.CreatedOn = createdOn

	return result
}

// InsertWebhookResults will insert the passed in webhook results, setting the ID parameter on each
func InsertWebhookResults(ctx context.Context, db Queryer, results []*WebhookResult) error {
	// convert to interface arrray
	is := make([]interface{}, len(results))
	for i := range results {
		is[i] = &results[i].r
	}

	return BulkQuery(ctx, "inserting webhook results", db, insertWebhookResultsSQL, is)
}

const insertWebhookResultsSQL = `
INSERT INTO
api_webhookresult( org_id,  contact_id,  url,  request,  status_code,  response,  request_time,  created_on)
	   	   VALUES(:org_id, :contact_id, :url, :request, :status_code, :response, :request_time, :created_on)
RETURNING id
`
