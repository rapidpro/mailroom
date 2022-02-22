package testsuite

import (
	"io"
	"net/http"
	"net/http/httptest"
)

// MockElasticServer is a mock HTTP server/endpoint that can be used to test elastic queries
type MockElasticServer struct {
	Server          *httptest.Server
	LastRequestURL  string
	LastRequestBody string
	NextResponse    string
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

		w.WriteHeader(200)
		w.Write([]byte(m.NextResponse))
		m.NextResponse = ""
	}))
	return m
}

// Close closes our HTTP server
func (m *MockElasticServer) Close() {
	m.Server.Close()
}

// URL returns the URL to call this server
func (m *MockElasticServer) URL() string {
	return m.Server.URL
}
