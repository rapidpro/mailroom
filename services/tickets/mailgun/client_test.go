package mailgun_test

import (
	"net/http"
	"testing"

	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/goflow/utils/uuids"
	"github.com/nyaruka/mailroom/services/tickets/mailgun"

	"github.com/stretchr/testify/assert"
)

func TestSendMessage(t *testing.T) {
	defer httpx.SetRequestor(httpx.DefaultRequestor)
	defer uuids.SetGenerator(uuids.DefaultGenerator)

	httpx.SetRequestor(httpx.NewMockRequestor(map[string][]httpx.MockResponse{
		"https://api.mailgun.net/v3/tickets.rapidpro.io/messages": {
			httpx.MockConnectionError,
			httpx.NewMockResponse(400, nil, `{"message": "Something went wrong"}`), // non-200 response
			httpx.NewMockResponse(200, nil, `xx`),                                  // non-JSON response
			httpx.NewMockResponse(200, nil, `{
				"id": "<20200426161758.1.590432020254B2BF@tickets.rapidpro.io>",
				"message": "Queued. Thank you."
			}`),
		},
	}))
	uuids.SetGenerator(uuids.NewSeededGenerator(12345))

	client := mailgun.NewClient(http.DefaultClient, nil, "tickets.rapidpro.io", "123456789")

	_, _, err := client.SendMessage("Bob <ticket+12446@tickets.rapidpro.io>", "support@acme.com", "Need help", "Where are my cookies?", nil)
	assert.EqualError(t, err, "unable to connect to server")

	_, _, err = client.SendMessage("Bob <ticket+12446@tickets.rapidpro.io>", "support@acme.com", "Need help", "Where are my cookies?", nil)
	assert.EqualError(t, err, "Something went wrong")

	_, _, err = client.SendMessage("Bob <ticket+12446@tickets.rapidpro.io>", "support@acme.com", "Need help", "Where are my cookies?", nil)
	assert.EqualError(t, err, "invalid character 'x' looking for beginning of value")

	msgID, trace, err := client.SendMessage("Bob <ticket+12446@tickets.rapidpro.io>", "support@acme.com", "Need help", "Where are my cookies?", map[string]string{"In-Reply-To": "12415"})
	assert.NoError(t, err)
	assert.Equal(t, "<20200426161758.1.590432020254B2BF@tickets.rapidpro.io>", msgID)
	assert.Equal(t, "POST /v3/tickets.rapidpro.io/messages HTTP/1.1\r\nHost: api.mailgun.net\r\nUser-Agent: Go-http-client/1.1\r\nContent-Length: 586\r\nAuthorization: Basic YXBpOjEyMzQ1Njc4OQ==\r\nContent-Type: multipart/form-data; boundary=9688d21d-95aa-4bed-afc7-f31b35731a3d\r\nAccept-Encoding: gzip\r\n\r\n--9688d21d-95aa-4bed-afc7-f31b35731a3d\r\nContent-Disposition: form-data; name=\"from\"\r\n\r\nBob <ticket+12446@tickets.rapidpro.io>\r\n--9688d21d-95aa-4bed-afc7-f31b35731a3d\r\nContent-Disposition: form-data; name=\"to\"\r\n\r\nsupport@acme.com\r\n--9688d21d-95aa-4bed-afc7-f31b35731a3d\r\nContent-Disposition: form-data; name=\"subject\"\r\n\r\nNeed help\r\n--9688d21d-95aa-4bed-afc7-f31b35731a3d\r\nContent-Disposition: form-data; name=\"text\"\r\n\r\nWhere are my cookies?\r\n--9688d21d-95aa-4bed-afc7-f31b35731a3d\r\nContent-Disposition: form-data; name=\"h:In-Reply-To\"\r\n\r\n12415\r\n--9688d21d-95aa-4bed-afc7-f31b35731a3d--\r\n", string(trace.RequestTrace))
	assert.Equal(t, "HTTP/1.0 200 OK\r\nContent-Length: 111\r\n\r\n", string(trace.ResponseTrace))
}
