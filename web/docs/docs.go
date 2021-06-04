package docs

import (
	"context"
	"net/http"

	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/web"
)

var docServer http.Handler

func init() {
	docServer = http.StripPrefix("/mr/docs", http.FileServer(http.Dir("docs")))

	// redirect non slashed docs to slashed version so relative URLs work
	web.RegisterRoute(http.MethodGet, "/mr/docs", func(ctx context.Context, rt *runtime.Runtime, r *http.Request, rawW http.ResponseWriter) error {
		http.Redirect(rawW, r, "/mr/docs/", http.StatusMovedPermanently)
		return nil
	})

	// all slashed docs are served by our static dir
	web.RegisterRoute(http.MethodGet, "/mr/docs/*", handleDocs)
}

func handleDocs(ctx context.Context, rt *runtime.Runtime, r *http.Request, rawW http.ResponseWriter) error {
	docServer.ServeHTTP(rawW, r)
	return nil
}
