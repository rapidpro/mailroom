package twiml

import (
	"encoding/xml"
	"testing"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/goflow/flows/routers/waits"
	"github.com/nyaruka/goflow/flows/routers/waits/hints"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/config"

	"github.com/nyaruka/goflow/flows"
	"github.com/stretchr/testify/assert"
)

func TestResponseForSprint(t *testing.T) {
	// for tests it is more convenient to not have formatted output
	indentMarshal = false

	urn := urns.URN("tel:+12067799294")
	channelRef := assets.NewChannelReference(assets.ChannelUUID(uuids.New()), "Twilio Channel")

	resumeURL := "http://temba.io/resume?session=1"

	// set our attachment domain for testing
	config.Mailroom.AttachmentDomain = "mailroom.io"
	defer func() { config.Mailroom.AttachmentDomain = "" }()

	tcs := []struct {
		Events   []flows.Event
		Wait     flows.ActivatedWait
		Expected string
	}{
		{
			[]flows.Event{events.NewIVRCreated(flows.NewMsgOut(urn, channelRef, "hello world", nil, nil, nil, flows.NilMsgTopic))},
			nil,
			`<Response><Say>hello world</Say><Hangup></Hangup></Response>`,
		},
		{
			[]flows.Event{events.NewIVRCreated(flows.NewIVRMsgOut(urn, channelRef, "hello world", "eng", ""))},
			nil,
			`<Response><Say language="en-US">hello world</Say><Hangup></Hangup></Response>`,
		},
		{
			[]flows.Event{events.NewIVRCreated(flows.NewIVRMsgOut(urn, channelRef, "hello world", "ben", ""))},
			nil,
			`<Response><Say>hello world</Say><Hangup></Hangup></Response>`,
		},
		{
			[]flows.Event{events.NewIVRCreated(flows.NewMsgOut(urn, channelRef, "hello world", []utils.Attachment{utils.Attachment("audio:/recordings/foo.wav")}, nil, nil, flows.NilMsgTopic))},
			nil,
			`<Response><Play>https://mailroom.io/recordings/foo.wav</Play><Hangup></Hangup></Response>`,
		},
		{
			[]flows.Event{events.NewIVRCreated(flows.NewMsgOut(urn, channelRef, "hello world", []utils.Attachment{utils.Attachment("audio:https://temba.io/recordings/foo.wav")}, nil, nil, flows.NilMsgTopic))},
			nil,
			`<Response><Play>https://temba.io/recordings/foo.wav</Play><Hangup></Hangup></Response>`,
		},
		{
			[]flows.Event{
				events.NewIVRCreated(flows.NewMsgOut(urn, channelRef, "hello world", nil, nil, nil, flows.NilMsgTopic)),
				events.NewIVRCreated(flows.NewMsgOut(urn, channelRef, "goodbye", nil, nil, nil, flows.NilMsgTopic)),
			},
			nil,
			`<Response><Say>hello world</Say><Say>goodbye</Say><Hangup></Hangup></Response>`,
		},
		{
			[]flows.Event{events.NewIVRCreated(flows.NewMsgOut(urn, channelRef, "enter a number", nil, nil, nil, flows.NilMsgTopic))},
			waits.NewActivatedMsgWait(nil, hints.NewFixedDigitsHint(1)),
			`<Response><Gather numDigits="1" timeout="30" action="http://temba.io/resume?session=1&amp;wait_type=gather"><Say>enter a number</Say></Gather><Redirect>http://temba.io/resume?session=1&amp;wait_type=gather&amp;timeout=true</Redirect></Response>`,
		},
		{
			[]flows.Event{events.NewIVRCreated(flows.NewMsgOut(urn, channelRef, "enter a number, then press #", nil, nil, nil, flows.NilMsgTopic))},
			waits.NewActivatedMsgWait(nil, hints.NewTerminatedDigitsHint("#")),
			`<Response><Gather finishOnKey="#" timeout="30" action="http://temba.io/resume?session=1&amp;wait_type=gather"><Say>enter a number, then press #</Say></Gather><Redirect>http://temba.io/resume?session=1&amp;wait_type=gather&amp;timeout=true</Redirect></Response>`,
		},
		{
			[]flows.Event{events.NewIVRCreated(flows.NewMsgOut(urn, channelRef, "say something", nil, nil, nil, flows.NilMsgTopic))},
			waits.NewActivatedMsgWait(nil, hints.NewAudioHint()),
			`<Response><Say>say something</Say><Record action="http://temba.io/resume?session=1&amp;wait_type=record" maxLength="600"></Record><Redirect>http://temba.io/resume?session=1&amp;wait_type=record&amp;empty=true</Redirect></Response>`,
		},
	}

	for i, tc := range tcs {
		response, err := responseForSprint(urn, resumeURL, tc.Wait, tc.Events)
		assert.NoError(t, err, "%d: unexpected error")
		assert.Equal(t, xml.Header+tc.Expected, response, "%d: unexpected response", i)
	}
}
