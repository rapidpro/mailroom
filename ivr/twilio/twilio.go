package twilio

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"sort"
	"strings"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/mailroom/ivr"
	"github.com/nyaruka/mailroom/models"
	"github.com/pkg/errors"
)

const (
	twilioChannelType = models.ChanelType("T")

	baseURL  = `https://api.twilio.com`
	callPath = `/2010-04-01/Accounts/{AccountSid}/Calls.json`

	signatureHeader     = "X-Twilio-Signature"
	forwardedPathHeader = "X-Forwarded-Path"

	statusFailed = "failed"

	accountSIDConfig := "account_sid"
	authTokenConfig := "auth_token"
)

type client struct {
	baseURL    string
	accountSID string
	authToken  string
}

func init() {
	ivr.RegisterClient(twilioChannelType, NewClientFromChannel)
}

// NewClientFromChannel creates a new Twilio IVR client for the passed in account and and auth token
func NewClientFromChannel(channel *models.Channel) (ivr.IVRClient, error) {
	accountSID := channel.ConfigValue(accountSIDConfig, "")
	authToken := channel.ConfigValue(authTokenConfig, "")
	if accountSID == "" || authToken == "" {
		return nil, errors.Errorf("missing auth_token or account_sid on channel config")
	}

	return &client{
		baseURL:    baseURL,
		accountSID: accountSID,
		authToken:  authToken,
	}, nil
}

// NewClient creates a new Twilio IVR client for the passed in account and and auth token
func NewClient(accountSID string, authToken string) ivr.IVRClient {
	return &client{
		baseURL:    baseURL,
		accountSID: accountSID,
		authToken:  authToken,
	}
}

// CallResponse is our struct for a Twilio call response
type CallResponse struct {
	SID    string `json:"sid"`
	Status string `json:"status"`
}

func (c *client) RequestCall(channel *models.Channel, number urns.URN, callbackURL string, statusURL string) (ivr.CallID, error) {
	form := url.Values{}
	form.Set("To", number.Path())
	form.Set("From", channel.Address())
	form.Set("Url", callbackURL)
	form.Set("StatusCallback", statusURL)

	sendURL := baseURL + strings.Replace(callPath, "{AccountSID}", c.accountSID, -1)

	resp, err := c.postRequest(sendURL, form)
	if err != nil {
		return ivr.NilCallID, errors.Wrapf(err, "error trying to start call")
	}

	if resp.StatusCode != 200 {
		return ivr.NilCallID, errors.Wrapf(err, "received non 200 status for call start: %d", resp.StatusCode)
	}

	// read our body
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return ivr.NilCallID, errors.Wrapf(err, "error reading response body")
	}

	// parse out our call sid
	call := &CallResponse{}
	err = json.Unmarshal(body, call)
	if err != nil || call.SID == "" {
		return ivr.NilCallID, errors.Errorf("unable to read call id")
	}

	if call.Status == statusFailed {
		return ivr.NilCallID, errors.Errorf("call status returned as failed")
	}

	return ivr.CallID(call.SID), nil
}

func (c *client) postRequest(sendURL string, form url.Values) (*http.Response, error) {
	req, _ := http.NewRequest(http.MethodPost, sendURL, strings.NewReader(form.Encode()))
	req.SetBasicAuth(c.accountSID, c.authToken)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	return http.DefaultClient.Do(req)
}

// see https://www.twilio.com/docs/api/security
func validateSignature(r *http.Request, authToken string) error {
	actual := r.Header.Get(signatureHeader)
	if actual == "" {
		return fmt.Errorf("missing request signature")
	}

	if err := r.ParseForm(); err != nil {
		return err
	}

	path := r.URL.RequestURI()
	proxyPath := r.Header.Get(forwardedPathHeader)
	if proxyPath != "" {
		path = proxyPath
	}

	url := fmt.Sprintf("https://%s%s", r.Host, path)
	expected, err := twCalculateSignature(url, r.PostForm, authToken)
	if err != nil {
		return err
	}

	// compare signatures in way that isn't sensitive to a timing attack
	if !hmac.Equal(expected, []byte(actual)) {
		return fmt.Errorf("invalid request signature")
	}

	return nil
}

// see https://www.twilio.com/docs/api/security
func twCalculateSignature(url string, form url.Values, authToken string) ([]byte, error) {
	var buffer bytes.Buffer
	buffer.WriteString(url)

	keys := make(sort.StringSlice, 0, len(form))
	for k := range form {
		keys = append(keys, k)
	}
	keys.Sort()

	for _, k := range keys {
		buffer.WriteString(k)
		for _, v := range form[k] {
			buffer.WriteString(v)
		}
	}

	// hash with SHA1
	mac := hmac.New(sha1.New, []byte(authToken))
	mac.Write(buffer.Bytes())
	hash := mac.Sum(nil)

	// encode with Base64
	encoded := make([]byte, base64.StdEncoding.EncodedLen(len(hash)))
	base64.StdEncoding.Encode(encoded, hash)

	return encoded, nil
}
