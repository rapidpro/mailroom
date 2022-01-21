package twilioflex

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/fatih/structs"
	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/gocommon/jsonx"
)

type baseClient struct {
	httpClient   *http.Client
	httpRetries  *httpx.RetryConfig
	authToken    string
	accountSID   string
	serviceSID   string
	workspaceSID string
}

func newBaseClient(httpClient *http.Client, httpRetries *httpx.RetryConfig, authToken, accountSID, workspaceSID string) baseClient {
	return baseClient{
		httpClient:   httpClient,
		httpRetries:  httpRetries,
		authToken:    authToken,
		accountSID:   accountSID,
		workspaceSID: workspaceSID,
	}
}

type errorResponse struct {
	Error       string `json:"error"`
	Description string `json:"description"`
}

func (c *baseClient) request(method, url string, payload interface{}, response interface{}) (*httpx.Trace, error) {
	headers := map[string]string{}

	formValues := structToMap(payload)
	body := strings.NewReader(formValues.Encode())

	req, err := httpx.NewRequest(method, url, body, headers)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(c.accountSID, c.authToken)

	trace, err := httpx.DoTrace(c.httpClient, req, c.httpRetries, nil, -1)
	if err != nil {
		return trace, err
	}

	if trace.Response.StatusCode >= 400 {
		response := &errorResponse{}
		jsonx.Unmarshal(trace.ResponseBody, response)
		return trace, errors.New(response.Description)
	}

	if response != nil {
		return trace, jsonx.Unmarshal(trace.ResponseBody, response)
	}
	return nil, nil
}

func (c *baseClient) post(url string, payload, response interface{}) (*httpx.Trace, error) {
	return c.request("POST", url, payload, response)
}

func (c *baseClient) put(url string, payload, response interface{}) (*httpx.Trace, error) {
	return c.request("PUT", url, payload, response)
}

func (c *baseClient) delete(url string, payload, response interface{}) (*httpx.Trace, error) {
	return c.request("DELETE", url, payload, response)
}

func (c *baseClient) get(url string, payload, response interface{}) (*httpx.Trace, error) {
	return c.request("GET", url, payload, response)
}

type RESTClient struct {
	baseClient
}

func NewRestClient(httpClient *http.Client, httpRetries *httpx.RetryConfig, authToken, accountSID, workspaceSID string) *RESTClient {
	return &RESTClient{
		baseClient: newBaseClient(httpClient, httpRetries, authToken, accountSID, workspaceSID),
	}
}

func (c *RESTClient) CreateUser(user *ChatUser) (*ChatUser, *httpx.Trace, error) {
	url := fmt.Sprintf("https://chat.twilio.com/v2/Services/%s/Users", c.serviceSID)
	payload := structs.Map(user)

	response := &ChatUser{}
	trace, err := c.post(url, payload, response)
	if err != nil {
		return nil, trace, err
	}
	return response, trace, nil
}

func (c *RESTClient) CreateChannel(channel *ChatChannel) (*ChatChannel, *httpx.Trace, error) {
	// TODO: CreateChannel
	return nil, nil, nil
}

func (c *RESTClient) CreateChannelMemberFromUser(user *ChatUser) (*ChatMember, *httpx.Trace, error) {
	// TODO: CreateMember
	return nil, nil, nil
}

func (c *RESTClient) SetChannelWebhook() {
	// TODO: SetChannelWebhook
}

func (c *RESTClient) CreateMessage() {
	// TODO: CreateMessage
}

func (c *RESTClient) CreateTask() {
	// TODO: CreateTask
}

type ChatUser struct {
	AccountSID   *string                 `json:"account_sid,omitempty"`
	Attributes   *string                 `json:"attributes,omitempty"`
	DateCreated  *time.Time              `json:"date_created,omitempty"`
	DateUpdated  *time.Time              `json:"date_updated,omitempty"`
	FriendlyName *string                 `json:"friendly_name,omitempty"`
	Identity     *string                 `json:"identity,omitempty"`
	Links        *map[string]interface{} `json:"links,omitempty"`
	RoleSID      *string                 `json:"role_sid,omitempty"`
	ServiceSID   *string                 `json:"service_sid,omitempty"`
	SID          *string                 `json:"sid,omitempty"`
	Url          *string                 `json:"url,omitempty"`
}

type ChatChannel struct {
	AccountSID    *string                 `json:"account_sid,omitempty"`
	Attributes    *string                 `json:"attributes,omitempty"`
	CreatedBy     *string                 `json:"created_by,omitempty"`
	DateCreated   *time.Time              `json:"date_created,omitempty"`
	DateUpdated   *time.Time              `json:"date_updated,omitempty"`
	FriendlyName  *string                 `json:"friendly_name,omitempty"`
	Links         *map[string]interface{} `json:"links,omitempty"`
	MemberCount   *int                    `json:"member_count,omitempty"`
	MessagesCount *int                    `json:"messages_count,omitempty"`
	ServiceSID    *string                 `json:"service_sid,omitempty"`
	SID           *string                 `json:"sid,omitempty"`
	Type          *string                 `json:"type,omitempty"`
	UniqueName    *string                 `json:"unique_name,omitempty"`
}

type ChatMember struct {
	AccountSID               *string    `json:"account_sid,omitempty"`
	Attributes               *string    `json:"attributes,omitempty"`
	ChannelSID               *string    `json:"channel_sid,omitempty"`
	DateCreated              *time.Time `json:"date_created,omitempty"`
	DateUpdated              *time.Time `json:"date_updated,omitempty"`
	Identity                 *string    `json:"identity,omitempty"`
	LastConsumedMessageIndex *int       `json:"last_consumed_message_index,omitempty"`
	LastConsumptionTimestamp *time.Time `json:"last_consumption_timestamp,omitempty"`
	RoleSID                  *string    `json:"role_sid,omitempty"`
	ServiceSID               *string    `json:"service_sid,omitempty"`
	SID                      *string    `json:"sid,omitempty"`
	Url                      *string    `json:"url,omitempty"`
}

type ChatMessage struct {
	AccountSID    *string                 `json:"account_sid,omitempty"`
	Attributes    *string                 `json:"attributes,omitempty"`
	Body          *string                 `json:"body,omitempty"`
	ChannelSID    *string                 `json:"channel_sid,omitempty"`
	DateCreated   *time.Time              `json:"date_created,omitempty"`
	DateUpdated   *time.Time              `json:"date_updated,omitempty"`
	From          *string                 `json:"from,omitempty"`
	Index         *int                    `json:"index,omitempty"`
	LastUpdatedBy *string                 `json:"last_updated_by,omitempty"`
	Media         *map[string]interface{} `json:"media,omitempty"`
	ServiceSID    *string                 `json:"service_sid,omitempty"`
	SID           *string                 `json:"sid,omitempty"`
	To            *string                 `json:"to,omitempty"`
	Type          *string                 `json:"type,omitempty"`
	Url           *string                 `json:"url,omitempty"`
	WasEdited     *bool                   `json:"was_edited,omitempty"`
}

type ChatChannelWebhook struct {
	AccountSID    *string                 `json:"account_sid,omitempty"`
	ChannelSID    *string                 `json:"channel_sid,omitempty"`
	Configuration *map[string]interface{} `json:"configuration,omitempty"`
	DateCreated   *time.Time              `json:"date_created,omitempty"`
	DateUpdated   *time.Time              `json:"date_updated,omitempty"`
	ServiceSID    *string                 `json:"service_sid,omitempty"`
	SID           *string                 `json:"sid,omitempty"`
	Type          *string                 `json:"type,omitempty"`
	Url           *string                 `json:"url,omitempty"`
}

type TaskrouterTask struct {
	AccountSID            *string                 `json:"account_sid,omitempty"`
	Addons                *string                 `json:"addons,omitempty"`
	Age                   *int                    `json:"age,omitempty"`
	AssignmentStatus      *string                 `json:"assignment_status,omitempty"`
	Attributes            *string                 `json:"attributes,omitempty"`
	DateCreated           *time.Time              `json:"date_created,omitempty"`
	DateUpdated           *time.Time              `json:"date_updated,omitempty"`
	Links                 *map[string]interface{} `json:"links,omitempty"`
	Priority              *int                    `json:"priority,omitempty"`
	Reason                *string                 `json:"reason,omitempty"`
	SID                   *string                 `json:"sid,omitempty"`
	TaskChannelSID        *string                 `json:"task_channel_sid,omitempty"`
	TaskChannelUniqueName *string                 `json:"task_channel_unique_name,omitempty"`
	TaskQueueEnteredDate  *time.Time              `json:"task_queue_entered_date,omitempty"`
	TaskQueueFriendlyName *string                 `json:"task_queue_friendly_name,omitempty"`
	TaskQueueSID          *string                 `json:"task_queue_sid,omitempty"`
	Timeout               *int                    `json:"timeout,omitempty"`
	Url                   *string                 `json:"url,omitempty"`
	WorkflowFriendlyName  *string                 `json:"workflow_friendly_name,omitempty"`
	WorkflowSID           *string                 `json:"workflow_sid,omitempty"`
	WorkspaceSID          *string                 `json:"workspace_sid,omitempty"`
}

func structToMap(i interface{}) (values url.Values) {
	values = url.Values{}
	iVal := reflect.ValueOf(i).Elem()
	typ := iVal.Type()
	for i := 0; i < iVal.NumField(); i++ {
		f := iVal.Field(i)
		var v string
		switch f.Interface().(type) {
		case int, int8, int16, int32, int64:
			v = strconv.FormatInt(f.Int(), 10)
		case uint, uint8, uint16, uint32, uint64:
			v = strconv.FormatUint(f.Uint(), 10)
		case float32:
			v = strconv.FormatFloat(f.Float(), 'f', 4, 32)
		case float64:
			v = strconv.FormatFloat(f.Float(), 'f', 4, 64)
		case []byte:
			v = string(f.Bytes())
		case string:
			v = f.String()
		}
		values.Set(typ.Field(i).Name, v)
	}
	return
}
