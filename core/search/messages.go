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

// MessageDoc represents a message document in the OpenSearch messages index. UUID is used as the document _id
// and OrgID as the routing value, so neither are stored in the document body.
type MessageDoc struct {
	CreatedOn   time.Time         `json:"-"` // used to determine monthly index
	UUID        flows.EventUUID   `json:"-"` // used as _id
	OrgID       models.OrgID      `json:"-"` // used as routing value
	ContactUUID flows.ContactUUID `json:"contact_uuid"`
	Text        string            `json:"text"`
}

// IndexName returns the monthly index name for this message, e.g. base "messages" with a message
// from January 2026 gives "messages-2026-01".
func (m *MessageDoc) IndexName(base string) string {
	return fmt.Sprintf("%s-%s", base, m.CreatedOn.UTC().Format("2006-01"))
}

// MessageResult is a single result from a message search containing the contact UUID and event data.
type MessageResult struct {
	ContactUUID flows.ContactUUID
	Event       map[string]any
}

// SearchMessages searches the OpenSearch messages index for messages matching the given text in the given org,
// then fetches the corresponding events from DynamoDB.
func SearchMessages(ctx context.Context, rt *runtime.Runtime, orgID models.OrgID, text string, contactUUID flows.ContactUUID) ([]MessageResult, int, error) {
	routing := fmt.Sprintf("%d", orgID)

	must := []elastic.Query{elastic.Term("_routing", routing), elastic.Match("text", text)}
	if contactUUID != "" {
		must = append(must, elastic.Term("contact_uuid", contactUUID))
	}

	src := map[string]any{
		"query":            elastic.All(must...),
		"sort":             []any{"_score", map[string]string{"_id": "desc"}},
		"size":             50,
		"track_total_hits": true,
	}

	resp, err := rt.OS.Client.Search(ctx, &opensearchapi.SearchReq{
		Indices: []string{rt.Config.OSMessagesIndex + "-*"},
		Body:    bytes.NewReader(jsonx.MustMarshal(src)),
		Params:  opensearchapi.SearchParams{Routing: []string{routing}},
	})
	if err != nil {
		return nil, 0, fmt.Errorf("error searching messages: %w", err)
	}

	type hitResult struct {
		uuid        flows.EventUUID
		contactUUID flows.ContactUUID
	}

	hits := make([]hitResult, len(resp.Hits.Hits))
	for i, hit := range resp.Hits.Hits {
		var doc MessageDoc
		if err := json.Unmarshal(hit.Source, &doc); err != nil {
			return nil, 0, fmt.Errorf("error unmarshalling message doc: %w", err)
		}
		hits[i] = hitResult{uuid: flows.EventUUID(hit.ID), contactUUID: doc.ContactUUID}
	}

	// build DynamoDB keys from OpenSearch results
	keys := make([]dynamo.Key, len(hits))
	for i, hit := range hits {
		keys[i] = dynamo.Key{
			PK: fmt.Sprintf("con#%s", hit.contactUUID),
			SK: fmt.Sprintf("evt#%s", hit.uuid),
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
	results := make([]MessageResult, 0, len(hits))
	for _, hit := range hits {
		item := itemsBySK[fmt.Sprintf("evt#%s", hit.uuid)]
		if item == nil {
			continue
		}

		data, err := item.GetData()
		if err != nil {
			return nil, 0, fmt.Errorf("error getting event data: %w", err)
		}

		data["uuid"] = string(hit.uuid) // re-add uuid (stripped on write)

		results = append(results, MessageResult{
			ContactUUID: hit.contactUUID,
			Event:       data,
		})
	}

	return results, resp.Hits.Total.Value, nil
}

// DeindexMessagesByContact deletes all messages in the OpenSearch messages index for the given contact UUIDs.
func DeindexMessagesByContact(ctx context.Context, rt *runtime.Runtime, orgID models.OrgID, contactUUIDs []flows.ContactUUID) (int, error) {
	if rt.OS == nil {
		return 0, nil
	}

	routing := fmt.Sprintf("%d", orgID)
	uuids := make([]string, len(contactUUIDs))
	for i, u := range contactUUIDs {
		uuids[i] = string(u)
	}

	src := map[string]any{
		"query": map[string]any{
			"terms": map[string]any{"contact_uuid": uuids},
		},
	}

	resp, err := rt.OS.Client.Document.DeleteByQuery(ctx, opensearchapi.DocumentDeleteByQueryReq{
		Indices: []string{rt.Config.OSMessagesIndex + "-*"},
		Body:    bytes.NewReader(jsonx.MustMarshal(src)),
		Params:  opensearchapi.DocumentDeleteByQueryParams{Routing: []string{routing}},
	})
	if err != nil {
		return 0, fmt.Errorf("error deindexing messages for contacts in org #%d: %w", orgID, err)
	}

	return resp.Deleted, nil
}
