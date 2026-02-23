package search

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nyaruka/gocommon/aws/dynamo"
	"github.com/nyaruka/gocommon/elastic"
	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
)

const (
	// MessageTextMinLength is the minimum length of message text to be indexed
	MessageTextMinLength = 2
)

// MessageDoc represents a message document in the OpenSearch messages index
type MessageDoc struct {
	Timestamp   time.Time         `json:"@timestamp"`
	OrgID       models.OrgID      `json:"org_id"`
	UUID        flows.EventUUID   `json:"uuid"`
	ContactUUID flows.ContactUUID `json:"contact_uuid"`
	Text        string            `json:"text"`
}

// MessageResult is a single result from a message search containing the contact UUID and event data.
type MessageResult struct {
	ContactUUID flows.ContactUUID
	Event       map[string]any
}

// SearchMessages searches the OpenSearch messages index for messages matching the given text in the given org,
// then fetches the corresponding events from DynamoDB.
func SearchMessages(ctx context.Context, rt *runtime.Runtime, orgID models.OrgID, text string) ([]MessageResult, int, error) {
	if rt.Search == nil {
		return nil, 0, fmt.Errorf("OpenSearch not configured")
	}

	src := map[string]any{
		"query":            elastic.All(elastic.Term("org_id", orgID), elastic.Match("text", text)),
		"sort":             []any{"_score", elastic.SortBy("@timestamp", false)},
		"size":             50,
		"track_total_hits": true,
	}

	resp, err := rt.Search.Messages.Client().Search(ctx, &opensearchapi.SearchReq{
		Indices: []string{rt.Search.Messages.Index()},
		Body:    bytes.NewReader(jsonx.MustMarshal(src)),
	})
	if err != nil {
		return nil, 0, fmt.Errorf("error searching messages: %w", err)
	}

	docs := make([]MessageDoc, len(resp.Hits.Hits))
	for i, hit := range resp.Hits.Hits {
		if err := json.Unmarshal(hit.Source, &docs[i]); err != nil {
			return nil, 0, fmt.Errorf("error unmarshalling message doc: %w", err)
		}
	}

	// build DynamoDB keys from OpenSearch results
	keys := make([]dynamo.Key, len(docs))
	for i, doc := range docs {
		keys[i] = dynamo.Key{
			PK: fmt.Sprintf("con#%s", doc.ContactUUID),
			SK: fmt.Sprintf("evt#%s", doc.UUID),
		}
	}

	// batch fetch events from DynamoDB
	items, _, err := dynamo.BatchGetItem(ctx, rt.Dynamo.History.Client(), rt.Dynamo.History.Table(), keys)
	if err != nil {
		return nil, 0, fmt.Errorf("error fetching events from DynamoDB: %w", err)
	}

	// index items by SK for ordered lookup
	itemsBySK := make(map[string]*dynamo.Item, len(items))
	for _, item := range items {
		itemsBySK[item.SK] = item
	}

	// build results in OpenSearch relevance order, skipping any not found in DynamoDB
	results := make([]MessageResult, 0, len(docs))
	for _, doc := range docs {
		item := itemsBySK[fmt.Sprintf("evt#%s", doc.UUID)]
		if item == nil {
			continue
		}

		data, err := item.GetData()
		if err != nil {
			return nil, 0, fmt.Errorf("error getting event data: %w", err)
		}

		data["uuid"] = string(doc.UUID) // re-add uuid (stripped on write)

		results = append(results, MessageResult{
			ContactUUID: doc.ContactUUID,
			Event:       data,
		})
	}

	return results, resp.Hits.Total.Value, nil
}
