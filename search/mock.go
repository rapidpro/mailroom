package search

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
)

// MockElasticServer is a mock HTTP server/endpoint that can be used to test elastic queries
type MockElasticServer struct {
	Server       *httptest.Server
	LastBody     string
	NextResponse string
}

// NewMockElasticServer creates a new mock elastic server
func NewMockElasticServer() *MockElasticServer {
	mock := &MockElasticServer{}
	mock.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
		body, _ := ioutil.ReadAll(r.Body)
		mock.LastBody = string(body)

		w.WriteHeader(200)
		w.Write([]byte(mock.NextResponse))
		mock.NextResponse = ""
	}))
	return mock
}

// Close closes our HTTP server
func (m *MockElasticServer) Close() {
	m.Server.Close()
}

// URL returns the URL to call this server
func (m *MockElasticServer) URL() string {
	return m.Server.URL
}
