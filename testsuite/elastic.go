package testsuite

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"

	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/olivere/elastic/v7"
)

// MockElasticServer is a mock HTTP server/endpoint that can be used to test elastic queries
type MockElasticServer struct {
	Server          *httptest.Server
	LastRequestURL  string
	LastRequestBody string
	Responses       [][]byte
}

// NewMockElasticServer creates a new mock elastic server
func NewMockElasticServer() *MockElasticServer {
	m := &MockElasticServer{}
	m.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		m.LastRequestURL = r.URL.String()

		// scrolling of results, we are always one page, so return empty hits
		if r.URL.String() == "/_search/scroll" {
			w.WriteHeader(200)
			w.Write([]byte(`
			{
				"_scroll_id": "anything==",
				"took": 7,
				"timed_out": false,
				"_shards": {
				  "total": 1,
				  "successful": 1,
				  "skipped": 0,
				  "failed": 0
				},
				"hits": {
				  "total": 1000,
				  "max_score": null,
				  "hits": []
				}
			}
			`))
			return
		}

		// otherwise read our next body and return our next response
		body, _ := io.ReadAll(r.Body)
		m.LastRequestBody = string(body)

		if len(m.Responses) == 0 {
			panic("mock elastic server has no more queued responses")
		}

		var response []byte
		response, m.Responses = m.Responses[0], m.Responses[1:]

		w.WriteHeader(200)
		w.Write(response)
	}))
	return m
}

func (m *MockElasticServer) Client() *elastic.Client {
	c, _ := elastic.NewClient(elastic.SetURL(m.URL()), elastic.SetHealthcheck(false), elastic.SetSniff(false))
	return c
}

// Close closes our HTTP server
func (m *MockElasticServer) Close() {
	m.Server.Close()
}

// URL returns the URL to call this server
func (m *MockElasticServer) URL() string {
	return m.Server.URL
}

// AddResponse adds a mock response to the server's queue
func (m *MockElasticServer) AddResponse(ids ...models.ContactID) {
	hits := make([]map[string]interface{}, len(ids))
	for i := range ids {
		hits[i] = map[string]interface{}{
			"_index":   "contacts",
			"_type":    "_doc",
			"_id":      fmt.Sprintf("%d", ids[i]),
			"_score":   nil,
			"_routing": "1",
			"sort":     []int{15124352},
		}
	}

	response := jsonx.MustMarshal(map[string]interface{}{
		"_scroll_id": "DXF1ZXJ5QW5kRmV0Y2gBAAAAAAAbgc0WS1hqbHlfb01SM2lLTWJRMnVOSVZDdw==",
		"took":       2,
		"timed_out":  false,
		"_shards": map[string]interface{}{
			"total":      1,
			"successful": 1,
			"skipped":    0,
			"failed":     0,
		},
		"hits": map[string]interface{}{
			"total":     len(ids),
			"max_score": nil,
			"hits":      hits,
		},
	})
	m.Responses = append(m.Responses, response)
}
