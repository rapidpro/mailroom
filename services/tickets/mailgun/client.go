package mailgun

import (
	"bytes"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"sort"

	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/utils"
	"github.com/pkg/errors"
)

const apiBaseURL = "https://api.mailgun.net/v3"

type baseResponse struct {
	Message string `json:"message"`
}

// Client is a basic mailgun client
type Client struct {
	httpClient  *http.Client
	httpRetries *httpx.RetryConfig
	domain      string
	apiKey      string
}

// NewClient creates a new mailgun client
func NewClient(httpClient *http.Client, httpRetries *httpx.RetryConfig, domain, apiKey string) *Client {
	return &Client{
		httpClient:  httpClient,
		httpRetries: httpRetries,
		domain:      domain,
		apiKey:      apiKey,
	}
}

type messageResponse struct {
	baseResponse
	ID string `json:"id"`
}

// EmailAttachment is an email attachment
type EmailAttachment struct {
	Filename    string
	ContentType string
	Body        io.Reader
}

// SendMessage sends a new email message and returns the ID
// see https://documentation.mailgun.com/en/latest/api-sending.html
func (c *Client) SendMessage(from, to, subject, text string, attachments []*EmailAttachment, headers map[string]string) (string, *httpx.Trace, error) {
	writeBody := func(w *multipart.Writer) error {
		w.WriteField("from", from)
		w.WriteField("to", to)
		w.WriteField("subject", subject)
		w.WriteField("text", text)

		for _, attachment := range attachments {
			h := make(textproto.MIMEHeader)
			h.Set("Content-Disposition", fmt.Sprintf(`form-data; name="attachment"; filename="%s"`, attachment.Filename))
			h.Set("Content-Type", attachment.ContentType)
			fw, err := w.CreatePart(h)
			if err != nil {
				return err
			}
			_, err = io.Copy(fw, attachment.Body)
			if err != nil {
				return err
			}
		}

		// for the sake of tests, we want to output headers in consistent order
		headerKeys := make([]string, 0, len(headers))
		for k := range headers {
			headerKeys = append(headerKeys, k)
		}
		sort.Strings(headerKeys)

		for _, k := range headerKeys {
			w.WriteField("h:"+k, headers[k])
		}
		return nil
	}

	trace, err := c.post("messages", writeBody)
	if err != nil {
		return "", trace, err
	}

	if trace.Response.StatusCode >= 400 {
		response := &baseResponse{}
		jsonx.Unmarshal(trace.ResponseBody, response)
		return "", trace, errors.New(response.Message)
	}

	response := &messageResponse{}
	if err := utils.UnmarshalAndValidate(trace.ResponseBody, response); err != nil {
		return "", trace, err
	}

	return response.ID, trace, nil
}

func (c *Client) post(endpoint string, payload func(w *multipart.Writer) error) (*httpx.Trace, error) {
	b := &bytes.Buffer{}
	w := multipart.NewWriter(b)
	w.SetBoundary(string(uuids.New()))

	if err := payload(w); err != nil {
		return nil, err
	}

	w.Close()

	req, err := http.NewRequest("POST", fmt.Sprintf("%s/%s/%s", apiBaseURL, c.domain, endpoint), bytes.NewReader(b.Bytes()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", w.FormDataContentType())
	req.SetBasicAuth("api", c.apiKey)

	return httpx.DoTrace(c.httpClient, req, c.httpRetries, nil, -1)
}
