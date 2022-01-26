package twilioflex

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/google/go-querystring/query"
	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/gocommon/jsonx"
)

type baseClient struct {
	httpClient     *http.Client
	httpRetries    *httpx.RetryConfig
	authToken      string
	accountSid     string
	serviceSid     string
	workspaceSid   string
	workflowSid    string
	taskChannelSid string
	flexFlowSid    string
}

func newBaseClient(httpClient *http.Client, httpRetries *httpx.RetryConfig, authToken, accountSid, serviceSid, workspaceSid, workflowSid, taskChannelSid, flexFlowSid string) baseClient {
	return baseClient{
		httpClient:     httpClient,
		httpRetries:    httpRetries,
		authToken:      authToken,
		accountSid:     accountSid,
		serviceSid:     serviceSid,
		workspaceSid:   workspaceSid,
		workflowSid:    workflowSid,
		taskChannelSid: taskChannelSid,
		flexFlowSid:    flexFlowSid,
	}
}

type errorResponse struct {
	Code     int32  `json:"code,omitempty"`
	Message  string `json:"message,omitempty"`
	MoreInfo string `json:"more_info,omitempty"`
	Status   int32  `json:"status,omitempty"`
}

func (c *baseClient) request(method, url string, payload url.Values, response interface{}) (*httpx.Trace, error) {
	data := strings.NewReader(payload.Encode())
	req, err := httpx.NewRequest(method, url, data, map[string]string{})
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(c.accountSid, c.authToken)
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Add("Content-Length", strconv.Itoa(len(payload.Encode())))

	trace, err := httpx.DoTrace(c.httpClient, req, c.httpRetries, nil, -1)
	if err != nil {
		return trace, err
	}

	if trace.Response.StatusCode >= 400 {
		response := &errorResponse{}
		jsonx.Unmarshal(trace.ResponseBody, response)
		return trace, errors.New(response.Message)
	}

	if response != nil {
		return trace, jsonx.Unmarshal(trace.ResponseBody, response)
	}
	return trace, nil
}

func (c *baseClient) post(url string, payload url.Values, response interface{}) (*httpx.Trace, error) {
	return c.request("POST", url, payload, response)
}

func (c *baseClient) put(url string, payload url.Values, response interface{}) (*httpx.Trace, error) {
	return c.request("PUT", url, payload, response)
}

func (c *baseClient) delete(url string, payload url.Values, response interface{}) (*httpx.Trace, error) {
	return c.request("DELETE", url, payload, response)
}

func (c *baseClient) get(url string, payload url.Values, response interface{}) (*httpx.Trace, error) {
	return c.request("GET", url, payload, response)
}

type RESTClient struct {
	baseClient
}

func NewRestClient(httpClient *http.Client, httpRetries *httpx.RetryConfig, authToken, accountSid, serviceSid, workspaceSid, workflowSid, taskChannelSid, flexFlowSid string) *RESTClient {
	return &RESTClient{
		baseClient: newBaseClient(httpClient, httpRetries, authToken, accountSid, serviceSid, workspaceSid, workflowSid, taskChannelSid, flexFlowSid),
	}
}

func (c *RESTClient) CreateUser(user *CreateChatUserParams) (*ChatUser, *httpx.Trace, error) {
	requestUrl := fmt.Sprintf("https://chat.twilio.com/v2/Services/%s/Users", c.serviceSid)
	response := &ChatUser{}
	data, err := query.Values(user)
	if err != nil {
		return nil, nil, err
	}
	trace, err := c.post(requestUrl, data, response)
	if err != nil {
		return nil, trace, err
	}
	return response, trace, nil
}

func (c *RESTClient) GetUser(userSid string) (*ChatUser, *httpx.Trace, error) {
	requestUrl := fmt.Sprintf("https://chat.twilio.com/v2/Services/%s/Users/%s", c.serviceSid, userSid)
	response := &ChatUser{}
	trace, err := c.post(requestUrl, url.Values{}, response)
	if err != nil {
		return nil, trace, err
	}
	return response, trace, nil

}

func (c *RESTClient) CreateFlexChannel(channel *CreateFlexChannelParams) (*FlexChannel, *httpx.Trace, error) {
	url := "https://flex-api.twilio.com/v1/Channels"
	response := &FlexChannel{}
	data, err := query.Values(channel)
	if err != nil {
		return nil, nil, err
	}
	data = removeEmpties(data)
	trace, err := c.post(url, data, response)
	if err != nil {
		return nil, trace, err
	}
	return response, trace, err
}

func (c *RESTClient) CreateFlexChannelWebhook(channelWebhook *CreateChatChannelWebhookParams, channelSid string) (*ChatChannelWebhook, *httpx.Trace, error) {
	requestUrl := fmt.Sprintf("https://chat.twilio.com/v2/Services/%s/Channels/%s/Webhooks", c.serviceSid, channelSid)
	response := &ChatChannelWebhook{}
	data := url.Values{
		"Configuration.Url":        []string{channelWebhook.ConfigurationUrl},
		"Configuration.Filters":    channelWebhook.ConfigurationFilters,
		"Configuration.Method":     []string{channelWebhook.ConfigurationMethod},
		"Configuration.RetryCount": []string{fmt.Sprint(channelWebhook.ConfigurationRetryCount)},
		"Type":                     []string{channelWebhook.Type},
	}
	trace, err := c.post(requestUrl, data, response)
	if err != nil {
		return nil, trace, err
	}
	return response, trace, err
}

func (c *RESTClient) CreateMessage(message *ChatMessage) (*ChatMessage, *httpx.Trace, error) {
	url := fmt.Sprintf("https://chat.twilio.com/v2/Services/%s/Channels/%s/Messages", c.serviceSid, message.ChannelSid)
	response := &ChatMessage{}
	data, err := query.Values(message)
	if err != nil {
		return nil, nil, err
	}
	data = removeEmpties(data)
	trace, err := c.post(url, data, response)
	if err != nil {
		return nil, trace, err
	}
	return response, trace, nil
}

type ChatUser struct {
	AccountSid   string                 `json:"account_sid,omitempty"`
	Attributes   string                 `json:"attributes,omitempty"`
	DateCreated  *time.Time             `json:"date_created,omitempty"`
	DateUpdated  *time.Time             `json:"date_updated,omitempty"`
	FriendlyName string                 `json:"friendly_name,omitempty"`
	Identity     string                 `json:"identity,omitempty"`
	Links        map[string]interface{} `json:"links,omitempty"`
	RoleSid      string                 `json:"role_sid,omitempty"`
	ServiceSid   string                 `json:"service_sid,omitempty"`
	Sid          string                 `json:"sid,omitempty"`
	Url          string                 `json:"url,omitempty"`
}

type CreateChatUserParams struct {
	XTwilioWebhookEnabled string `json:"X-Twilio-Webhook-Enabled,omitempty"`
	Attributes            string `json:"Attributes,omitempty"`
	FriendlyName          string `json:"FriendlyName,omitempty"`
	Identity              string `json:"Identity,omitempty"`
	RoleSid               string `json:"RoleSid,omitempty"`
}

type ChatChannel struct {
	AccountSid    string                 `json:"account_sid,omitempty"`
	Attributes    string                 `json:"attributes,omitempty"`
	CreatedBy     string                 `json:"created_by,omitempty"`
	DateCreated   *time.Time             `json:"date_created,omitempty"`
	DateUpdated   *time.Time             `json:"date_updated,omitempty"`
	FriendlyName  string                 `json:"friendly_name,omitempty"`
	Links         map[string]interface{} `json:"links,omitempty"`
	MemberCount   int                    `json:"member_count,omitempty"`
	MessagesCount int                    `json:"messages_count,omitempty"`
	ServiceSid    string                 `json:"service_sid,omitempty"`
	Sid           string                 `json:"sid,omitempty"`
	Type          string                 `json:"type,omitempty"`
	UniqueName    string                 `json:"unique_name,omitempty"`
}

type FlexChannel struct {
	AccountSid  string     `json:"account_sid,omitempty"`
	DateCreated *time.Time `json:"date_created,omitempty"`
	DateUpdated *time.Time `json:"date_updated,omitempty"`
	FlexFlowSid string     `json:"flex_flow_sid,omitempty"`
	Sid         string     `json:"sid,omitempty"`
	TaskSid     string     `json:"task_sid,omitempty"`
	Url         string     `json:"url,omitempty"`
	UserSid     string     `json:"user_sid,omitempty"`
}

type CreateFlexChannelParams struct {
	ChatFriendlyName     string `json:"ChatFriendlyName,omitempty"`
	ChatUniqueName       string `json:"ChatUniqueName,omitempty"`
	ChatUserFriendlyName string `json:"ChatUserFriendlyName,omitempty"`
	FlexFlowSid          string `json:"FlexFlowSid,omitempty"`
	Identity             string `json:"Identity,omitempty"`
	LongLived            bool   `json:"LongLived,omitempty"`
	PreEngagementData    string `json:"PreEngagementData,omitempty"`
	Target               string `json:"Target,omitempty"`
	TaskAttributes       string `json:"TaskAttributes,omitempty"`
	TaskSid              string `json:"TaskSid,omitempty"`
}

type ChatMember struct {
	AccountSid               string     `json:"account_sid,omitempty"`
	Attributes               string     `json:"attributes,omitempty"`
	ChannelSid               string     `json:"channel_sid,omitempty"`
	DateCreated              *time.Time `json:"date_created,omitempty"`
	DateUpdated              *time.Time `json:"date_updated,omitempty"`
	Identity                 string     `json:"identity,omitempty"`
	LastConsumedMessageIndex int        `json:"last_consumed_message_index,omitempty"`
	LastConsumptionTimestamp *time.Time `json:"last_consumption_timestamp,omitempty"`
	RoleSid                  string     `json:"role_sid,omitempty"`
	ServiceSid               string     `json:"service_sid,omitempty"`
	Sid                      string     `json:"sid,omitempty"`
	Url                      string     `json:"url,omitempty"`
}

type ChatMessage struct {
	AccountSid    string                 `json:"account_sid,omitempty"`
	Attributes    string                 `json:"attributes,omitempty"`
	Body          string                 `json:"body,omitempty"`
	ChannelSid    string                 `json:"channel_sid,omitempty"`
	DateCreated   *time.Time             `json:"date_created,omitempty"`
	DateUpdated   *time.Time             `json:"date_updated,omitempty"`
	From          string                 `json:"from,omitempty"`
	Index         int                    `json:"index,omitempty"`
	LastUpdatedBy string                 `json:"last_updated_by,omitempty"`
	Media         map[string]interface{} `json:"media,omitempty"`
	ServiceSid    string                 `json:"service_sid,omitempty"`
	Sid           string                 `json:"sid,omitempty"`
	To            string                 `json:"to,omitempty"`
	Type          string                 `json:"type,omitempty"`
	Url           string                 `json:"url,omitempty"`
	WasEdited     bool                   `json:"was_edited,omitempty"`
}

type ChatChannelWebhook struct {
	AccountSid    string                 `json:"account_sid,omitempty"`
	ChannelSid    string                 `json:"channel_sid,omitempty"`
	Configuration map[string]interface{} `json:"configuration,omitempty"`
	DateCreated   *time.Time             `json:"date_created,omitempty"`
	DateUpdated   *time.Time             `json:"date_updated,omitempty"`
	ServiceSid    string                 `json:"service_sid,omitempty"`
	Sid           string                 `json:"sid,omitempty"`
	Type          string                 `json:"type,omitempty"`
	Url           string                 `json:"url,omitempty"`
}

type CreateChatChannelWebhookParams struct {
	ConfigurationFilters    []string `json:"Configuration.Filters,omitempty"`
	ConfigurationFlowSid    string   `json:"Configuration.FlowSid,omitempty"`
	ConfigurationMethod     string   `json:"Configuration.Method,omitempty"`
	ConfigurationRetryCount int      `json:"Configuration.RetryCount,omitempty"`
	ConfigurationTriggers   []string `json:"Configuration.Triggers,omitempty"`
	ConfigurationUrl        string   `json:"Configuration.Url,omitempty"`
	Type                    string   `json:"Type,omitempty"`
}

type TaskrouterTask struct {
	AccountSid            string                 `json:"account_sid,omitempty"`
	Addons                string                 `json:"addons,omitempty"`
	Age                   int                    `json:"age,omitempty"`
	AssignmentStatus      string                 `json:"assignment_status,omitempty"`
	Attributes            string                 `json:"attributes,omitempty"`
	DateCreated           *time.Time             `json:"date_created,omitempty"`
	DateUpdated           *time.Time             `json:"date_updated,omitempty"`
	Links                 map[string]interface{} `json:"links,omitempty"`
	Priority              int                    `json:"priority,omitempty"`
	Reason                string                 `json:"reason,omitempty"`
	Sid                   string                 `json:"sid,omitempty"`
	TaskChannel           string                 `json:"task_channel,omitempty"`
	TaskChannelUniqueName string                 `json:"task_channel_unique_name,omitempty"`
	TaskQueueEnteredDate  *time.Time             `json:"task_queue_entered_date,omitempty"`
	TaskQueueFriendlyName string                 `json:"task_queue_friendly_name,omitempty"`
	TaskQueueSid          string                 `json:"task_queue_sid,omitempty"`
	Timeout               int                    `json:"timeout,omitempty"`
	Url                   string                 `json:"url,omitempty"`
	WorkflowFriendlyName  string                 `json:"workflow_friendly_name,omitempty"`
	WorkflowSid           string                 `json:"workflow_sid,omitempty"`
	WorkspaceSid          string                 `json:"workspace_sid,omitempty"`
}

func removeEmpties(uv url.Values) url.Values {
	for k, v := range uv {
		if len(v) == 0 || len(v[0]) == 0 {
			delete(uv, k)
		}
	}
	return uv
}
