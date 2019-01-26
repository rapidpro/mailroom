package httputils

import (
	"net/http"
	"net/http/httputil"
	"time"

	"github.com/pkg/errors"
)

// RoundTrip represents a single request / response round trip created by our transport. In the
// case of connection errors, the status code will be set to 499 and the response body will
// contain more information about the error encountered
type RoundTrip struct {
	Method       string
	URL          string
	RequestBody  []byte
	Status       int
	ResponseBody []byte
	StartedOn    time.Time
	Elapsed      time.Duration
}

// LoggingTransport is a transport which keeps track of all requests and responses
type LoggingTransport struct {
	tripper    http.RoundTripper
	RoundTrips []*RoundTrip
}

// RoundTrip satisfier the roundtripper interface in http to allow for capturing
// requests and responses of the parent http client.
func (t *LoggingTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	rt := &RoundTrip{
		StartedOn: time.Now(),
		Method:    request.Method,
		URL:       request.URL.String(),
	}

	requestBody, err := httputil.DumpRequestOut(request, true)
	if err != nil {
		return nil, err
	}
	rt.RequestBody = requestBody
	t.RoundTrips = append(t.RoundTrips, rt)

	response, err := t.tripper.RoundTrip(request)
	rt.Elapsed = time.Since(rt.StartedOn)

	if err != nil {
		err = errors.Wrapf(err, "error making http request")
		rt.Status = 444
		rt.ResponseBody = []byte(err.Error())
		return response, err
	}

	defer response.Body.Close()

	responseBody, err := httputil.DumpResponse(response, true)
	if err != nil {
		err = errors.Wrapf(err, "error dumping http response")
		rt.Status = 444
		rt.ResponseBody = []byte(err.Error())
		return response, err
	}
	rt.ResponseBody = responseBody
	rt.Status = response.StatusCode

	return response, err
}

// NewLoggingTransport creates a new logging transport
func NewLoggingTransport(tripper http.RoundTripper) *LoggingTransport {
	return &LoggingTransport{
		tripper: tripper,
	}
}

// UserAgentTransport just injects a custom user agent on the request before sending it out
type UserAgentTransport struct {
	tripper http.RoundTripper
	agent   string
}

// RoundTrip just injects our custom user agent, passing the request down the chain
func (t *UserAgentTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	request.Header.Set("User-Agent", t.agent)
	return t.tripper.RoundTrip(request)
}

// NewUserAgentTransport creates a new transport that injects a user agent in all requests
func NewUserAgentTransport(tripper http.RoundTripper, agent string) *UserAgentTransport {
	return &UserAgentTransport{
		tripper: tripper,
		agent:   agent,
	}
}
