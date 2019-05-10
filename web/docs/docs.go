package docs

import (
	"context"
	"net/http"

	"github.com/nyaruka/mailroom/web"
)

var docServer http.Handler

func init() {
	docServer = http.StripPrefix("/docs/", http.FileServer(http.Dir("docs")))
	web.RegisterRoute(http.MethodGet, "/docs*", handleDocs)
}

func handleDocs(ctx context.Context, s *web.Server, r *http.Request, rawW http.ResponseWriter) error {
	docServer.ServeHTTP(rawW, r)
	return nil
}
