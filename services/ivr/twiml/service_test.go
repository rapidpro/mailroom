package twiml_test

import (
	"encoding/xml"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/goflow/flows/routers/waits/hints"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/services/ivr/twiml"
	"github.com/nyaruka/mailroom/testsuite"

	"github.com/nyaruka/goflow/flows"
	"github.com/stretchr/testify/assert"
)

func TestResponseForSprint(t *testing.T) {
	_, rt, _, _ := testsuite.Get()

	urn := urns.URN("tel:+12067799294")
	expiresOn := time.Now().Add(time.Hour)
	channelRef := assets.NewChannelReference(assets.ChannelUUID(uuids.New()), "Twilio Channel")

	resumeURL := "http://temba.io/resume?session=1"

	// set our attachment domain for testing
	rt.Config.AttachmentDomain = "mailroom.io"
	defer func() { rt.Config.AttachmentDomain = "" }()

	tcs := []struct {
		events   []flows.Event
		expected string
	}{
		{
			[]flows.Event{
				events.NewIVRCreated(flows.NewMsgOut(urn, channelRef, "hello world", nil, nil, nil, flows.NilMsgTopic)),
			},
			`<Response><Say>hello world</Say><Hangup></Hangup></Response>`,
		},
		{
			[]flows.Event{
				events.NewIVRCreated(flows.NewIVRMsgOut(urn, channelRef, "hello world", "eng", "")),
			},
			`<Response><Say language="en-US">hello world</Say><Hangup></Hangup></Response>`,
		},
		{
			[]flows.Event{
				events.NewIVRCreated(flows.NewIVRMsgOut(urn, channelRef, "hello world", "ben", "")),
			},
			`<Response><Say>hello world</Say><Hangup></Hangup></Response>`,
		},
		{
			[]flows.Event{
				events.NewIVRCreated(flows.NewMsgOut(urn, channelRef, "hello world", []utils.Attachment{utils.Attachment("audio:/recordings/foo.wav")}, nil, nil, flows.NilMsgTopic)),
			},
			`<Response><Play>https://mailroom.io/recordings/foo.wav</Play><Hangup></Hangup></Response>`,
		},
		{
			[]flows.Event{
				events.NewIVRCreated(flows.NewMsgOut(urn, channelRef, "hello world", []utils.Attachment{utils.Attachment("audio:https://temba.io/recordings/foo.wav")}, nil, nil, flows.NilMsgTopic)),
			},
			`<Response><Play>https://temba.io/recordings/foo.wav</Play><Hangup></Hangup></Response>`,
		},
		{
			[]flows.Event{
				events.NewIVRCreated(flows.NewMsgOut(urn, channelRef, "hello world", nil, nil, nil, flows.NilMsgTopic)),
				events.NewIVRCreated(flows.NewMsgOut(urn, channelRef, "goodbye", nil, nil, nil, flows.NilMsgTopic)),
			},
			`<Response><Say>hello world</Say><Say>goodbye</Say><Hangup></Hangup></Response>`,
		},
		{
			[]flows.Event{
				events.NewIVRCreated(flows.NewMsgOut(urn, channelRef, "enter a number", nil, nil, nil, flows.NilMsgTopic)),
				events.NewMsgWait(nil, nil, hints.NewFixedDigitsHint(1)),
			},
			`<Response><Gather numDigits="1" timeout="30" action="http://temba.io/resume?session=1&amp;wait_type=gather"><Say>enter a number</Say></Gather><Redirect>http://temba.io/resume?session=1&amp;wait_type=gather&amp;timeout=true</Redirect></Response>`,
		},
		{
			[]flows.Event{
				events.NewIVRCreated(flows.NewMsgOut(urn, channelRef, "enter a number, then press #", nil, nil, nil, flows.NilMsgTopic)),
				events.NewMsgWait(nil, nil, hints.NewTerminatedDigitsHint("#")),
			},
			`<Response><Gather finishOnKey="#" timeout="30" action="http://temba.io/resume?session=1&amp;wait_type=gather"><Say>enter a number, then press #</Say></Gather><Redirect>http://temba.io/resume?session=1&amp;wait_type=gather&amp;timeout=true</Redirect></Response>`,
		},
		{
			[]flows.Event{
				events.NewIVRCreated(flows.NewMsgOut(urn, channelRef, "say something", nil, nil, nil, flows.NilMsgTopic)),
				events.NewMsgWait(nil, nil, hints.NewAudioHint()),
			},
			`<Response><Say>say something</Say><Record action="http://temba.io/resume?session=1&amp;wait_type=record" maxLength="600"></Record><Redirect>http://temba.io/resume?session=1&amp;wait_type=record&amp;empty=true</Redirect></Response>`,
		},
		{
			[]flows.Event{
				events.NewDialWait(urns.URN(`tel:+1234567890`), &expiresOn),
			},
			`<Response><Dial action="http://temba.io/resume?session=1&amp;wait_type=dial">+1234567890</Dial></Response>`,
		},
	}

	for i, tc := range tcs {
		response, err := twiml.ResponseForSprint(rt.Config, urn, resumeURL, tc.events, false)
		assert.NoError(t, err, "%d: unexpected error")
		assert.Equal(t, xml.Header+tc.expected, response, "%d: unexpected response", i)
	}
}

func TestURNForRequest(t *testing.T) {
	s := twiml.NewService(http.DefaultClient, "12345", "sesame")

	makeRequest := func(body string) *http.Request {
		r, _ := http.NewRequest("POST", "http://nyaruka.com/12345/incoming", strings.NewReader(body))
		r.Header.Add("Content-Type", "application/x-www-form-urlencoded")
		r.Header.Add("Content-Length", strconv.Itoa(len(body)))
		return r
	}

	urn, err := s.URNForRequest(makeRequest(`CallSid=12345&AccountSid=23456&Caller=%2B12064871234&To=%2B12029795079&Called=%2B12029795079&CallStatus=queued&ApiVersion=2010-04-01&Direction=inbound`))
	assert.NoError(t, err)
	assert.Equal(t, urns.URN(`tel:+12064871234`), urn)

	// SignalWire uses From instead of Caller
	urn, err = s.URNForRequest(makeRequest(`CallSid=12345&AccountSid=23456&From=%2B12064871234&To=%2B12029795079&Called=%2B12029795079&CallStatus=queued&ApiVersion=2010-04-01&Direction=inbound`))
	assert.NoError(t, err)
	assert.Equal(t, urns.URN(`tel:+12064871234`), urn)

	_, err = s.URNForRequest(makeRequest(`CallSid=12345&AccountSid=23456&To=%2B12029795079&Called=%2B12029795079&CallStatus=queued&ApiVersion=2010-04-01&Direction=inbound`))
	assert.EqualError(t, err, "no Caller or From parameter found in request")
}
