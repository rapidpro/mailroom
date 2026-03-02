package system

import (
	"context"
	"net/http"

	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/web"
)

func init() {
	web.InternalRoute(http.MethodGet, "/system/latency", web.JSONPayload(handleLatency))
}

// Requests latency information for contact tasks by org.
//
//	{}
type latencyRequest struct {
}

// handles a request to get per-org contact task latency information
func handleLatency(ctx context.Context, rt *runtime.Runtime, r *latencyRequest) (any, int, error) {
	latencies, err := runtime.GetCTaskLatencies(rt.VK)
	if err != nil {
		return nil, 0, err
	}

	return latencies, http.StatusOK, nil
}
