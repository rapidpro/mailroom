package wenichats

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/goflow/assets"
	"github.com/pkg/errors"
)

type baseClient struct {
	httpClient  *http.Client
	httpRetries *httpx.RetryConfig
	authToken   string
	baseURL     string
}

func newBaseClient(httpClient *http.Client, httpRetries *httpx.RetryConfig, baseURL, authToken string) baseClient {

	return baseClient{
		httpClient:  httpClient,
		httpRetries: httpRetries,
		authToken:   authToken,
		baseURL:     baseURL,
	}
}

type errorResponse struct {
	Detail string `json:"detail"`
}

func (c *baseClient) request(method, url string, params *url.Values, payload, response interface{}) (*httpx.Trace, error) {
	pjson, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	data := strings.NewReader(string(pjson))
	req, err := httpx.NewRequest(method, url, data, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Authorization", "Bearer "+c.authToken)

	if params != nil {
		req.URL.RawQuery = params.Encode()
	}

	trace, err := httpx.DoTrace(c.httpClient, req, c.httpRetries, nil, -1)
	if err != nil {
		return trace, err
	}

	if trace.Response.StatusCode >= 400 {
		response := &errorResponse{}
		err = jsonx.Unmarshal(trace.ResponseBody, response)
		if err != nil {
			return trace, errors.Wrap(err, "couldn't parse error response")
		}
		return trace, errors.New(response.Detail)
	}

	if response != nil {
		err = json.Unmarshal(trace.ResponseBody, response)
		return trace, errors.Wrap(err, "couldn't parse response body")
	}

	return trace, nil
}

func (c *baseClient) post(url string, payload, response interface{}) (*httpx.Trace, error) {
	return c.request("POST", url, nil, payload, response)
}

func (c *baseClient) get(url string, params *url.Values, response interface{}) (*httpx.Trace, error) {
	return c.request("GET", url, params, nil, response)
}

func (c *baseClient) patch(url string, params *url.Values, payload, response interface{}) (*httpx.Trace, error) {
	return c.request("PATCH", url, nil, payload, response)
}

type Client struct {
	baseClient
}

func NewClient(httpClient *http.Client, httpRetries *httpx.RetryConfig, baseURL, authToken string) *Client {
	return &Client{
		baseClient: newBaseClient(httpClient, httpRetries, baseURL, authToken),
	}
}

func (c *Client) CreateRoom(room *RoomRequest) (*RoomResponse, *httpx.Trace, error) {
	url := c.baseURL + "/rooms/"
	response := &RoomResponse{}
	trace, err := c.post(url, room, response)
	if err != nil {
		return nil, trace, err
	}
	return response, trace, nil
}

func (c *Client) UpdateRoom(roomUUID string, room *RoomRequest) (*RoomResponse, *httpx.Trace, error) {
	url := fmt.Sprintf("%s/rooms/%s/", c.baseURL, roomUUID)
	response := &RoomResponse{}
	trace, err := c.patch(url, nil, room, response)
	if err != nil {
		return nil, trace, err
	}
	return response, trace, nil
}

func (c *Client) CloseRoom(roomUUID string) (*RoomResponse, *httpx.Trace, error) {
	url := fmt.Sprintf("%s/rooms/%s/close/", c.baseURL, roomUUID)
	response := &RoomResponse{}
	trace, err := c.patch(url, nil, nil, response)
	if err != nil {
		return nil, trace, err
	}
	return response, trace, nil
}

func (c *Client) CreateMessage(msg *MessageRequest) (*MessageResponse, *httpx.Trace, error) {
	url := fmt.Sprintf("%s/msgs/", c.baseURL)
	response := &MessageResponse{}
	trace, err := c.post(url, msg, response)
	if err != nil {
		return nil, trace, err
	}
	return response, trace, nil
}

func (c *Client) GetQueues(params *url.Values) (*QueuesResponse, *httpx.Trace, error) {
	url := fmt.Sprintf("%s/queues/", c.baseURL)
	response := &QueuesResponse{}
	trace, err := c.get(url, params, response)
	if err != nil {
		return nil, trace, err
	}
	return response, trace, nil
}

type RoomRequest struct {
	QueueUUID    string                 `json:"queue_uuid,omitempty"`
	UserEmail    string                 `json:"user_email,omitempty"`
	SectorUUID   string                 `json:"sector_uuid,omitempty"`
	Contact      *Contact               `json:"contact,omitempty"`
	CreatedOn    *time.Time             `json:"created_on,omitempty"`
	CustomFields map[string]interface{} `json:"custom_fields,omitempty"`
	CallbackURL  string                 `json:"callback_url,omitempty"`
	FlowUUID     assets.FlowUUID        `json:"flow_uuid,omitempty"`
	Groups       []Group                `json:"groups,omitempty"`
}

type Contact struct {
	ExternalID   string                 `json:"external_id,omitempty"`
	Name         string                 `json:"name,omitempty"`
	Email        string                 `json:"email,omitempty"`
	Phone        string                 `json:"phone,omitempty"`
	CustomFields map[string]interface{} `json:"custom_fields,omitempty"`
	URN          string                 `json:"urn,omitempty"`
}

type RoomResponse struct {
	UUID string `json:"uuid"`
	User struct {
		FirstName string `json:"first_name"`
		LastName  string `json:"last_name"`
		Email     string `json:"email"`
	} `json:"user"`
	Contact struct {
		ExternalID   string                 `json:"external_id"`
		Name         string                 `json:"name"`
		Email        string                 `json:"email"`
		Status       string                 `json:"status"`
		Phone        string                 `json:"phone"`
		CustomFields map[string]interface{} `json:"custom_fields"`
		CreatedOn    time.Time              `json:"created_on"`
	} `json:"contact"`
	Queue struct {
		UUID       string    `json:"uuid"`
		CreatedOn  time.Time `json:"created_on"`
		ModifiedOn time.Time `json:"modified_on"`
		Name       string    `json:"name"`
		Sector     string    `json:"sector"`
	} `json:"queue"`
	CreatedOn    time.Time              `json:"created_on"`
	ModifiedOn   time.Time              `json:"modified_on"`
	IsActive     bool                   `json:"is_active"`
	CustomFields map[string]interface{} `json:"custom_fields"`
	CallbackURL  string                 `json:"callback_url"`
}

type MessageRequest struct {
	Room        string       `json:"room"`
	Text        string       `json:"text"`
	CreatedOn   time.Time    `json:"created_on"`
	Direction   string       `json:"direction"`
	Attachments []Attachment `json:"attachments"`
}

type MessageResponse struct {
	UUID    string      `json:"uuid"`
	User    interface{} `json:"user"`
	Room    string      `json:"room"`
	Contact struct {
		UUID         string `json:"uuid"`
		Name         string `json:"name"`
		Email        string `json:"email"`
		Status       string `json:"status"`
		Phone        string `json:"phone"`
		CustomFields struct {
		} `json:"custom_fields"`
		CreatedOn time.Time `json:"created_on"`
	} `json:"contact"`
	Text      string       `json:"text"`
	Seen      bool         `json:"seen"`
	Media     []Attachment `json:"media"`
	CreatedOn string       `json:"created_on"`
}

type Attachment struct {
	ContentType string `json:"content_type"`
	URL         string `json:"url"`
}

type baseResponse struct {
	Count    int    `json:"count"`
	Next     string `json:"next"`
	Previous string `json:"previous"`
}

type QueuesResponse struct {
	baseResponse
	Results []Queue `json:"results"`
}

type Queue struct {
	UUID string `json:"uuid"`
	Name string `json:"name"`
}

type Group struct {
	UUID string `json:"uuid"`
	Name string `json:"name"`
}
