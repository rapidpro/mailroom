package nexmo

import (
	"net/http"
	"testing"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/goflow/flows/routers/waits"
	"github.com/nyaruka/goflow/flows/routers/waits/hints"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/goflow/utils/uuids"
	"github.com/nyaruka/mailroom/config"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/testsuite"

	"github.com/stretchr/testify/assert"
)

func TestResponseForSprint(t *testing.T) {
	ctx, db, rp := testsuite.Reset()
	rc := rp.Get()
	defer rc.Close()
	models.FlushCache()

	urn := urns.URN("tel:+12067799294")
	channelRef := assets.NewChannelReference(models.NexmoChannelUUID, "Nexmo Channel")

	resumeURL := "http://temba.io/resume?session=1"

	// deactivate our twilio channel
	db.MustExec(`UPDATE channels_channel SET is_active = FALSE WHERE id = $1`, models.TwilioChannelID)

	// add auth tokens
	db.MustExec(`UPDATE channels_channel SET config = '{"nexmo_app_id": "app_id", "nexmo_app_private_key": "-----BEGIN PRIVATE KEY-----\nMIICdgIBADANBgkqhkiG9w0BAQEFAASCAmAwggJcAgEAAoGBAKNwapOQ6rQJHetP\nHRlJBIh1OsOsUBiXb3rXXE3xpWAxAha0MH+UPRblOko+5T2JqIb+xKf9Vi3oTM3t\nKvffaOPtzKXZauscjq6NGzA3LgeiMy6q19pvkUUOlGYK6+Xfl+B7Xw6+hBMkQuGE\nnUS8nkpR5mK4ne7djIyfHFfMu4ptAgMBAAECgYA+s0PPtMq1osG9oi4xoxeAGikf\nJB3eMUptP+2DYW7mRibc+ueYKhB9lhcUoKhlQUhL8bUUFVZYakP8xD21thmQqnC4\nf63asad0ycteJMLb3r+z26LHuCyOdPg1pyLk3oQ32lVQHBCYathRMcVznxOG16VK\nI8BFfstJTaJu0lK/wQJBANYFGusBiZsJQ3utrQMVPpKmloO2++4q1v6ZR4puDQHx\nTjLjAIgrkYfwTJBLBRZxec0E7TmuVQ9uJ+wMu/+7zaUCQQDDf2xMnQqYknJoKGq+\noAnyC66UqWC5xAnQS32mlnJ632JXA0pf9pb1SXAYExB1p9Dfqd3VAwQDwBsDDgP6\nHD8pAkEA0lscNQZC2TaGtKZk2hXkdcH1SKru/g3vWTkRHxfCAznJUaza1fx0wzdG\nGcES1Bdez0tbW4llI5By/skZc2eE3QJAFl6fOskBbGHde3Oce0F+wdZ6XIJhEgCP\niukIcKZoZQzoiMJUoVRrA5gqnmaYDI5uRRl/y57zt6YksR3KcLUIuQJAd242M/WF\n6YAZat3q/wEeETeQq1wrooew+8lHl05/Nt0cCpV48RGEhJ83pzBm3mnwHf8lTBJH\nx6XroMXsmbnsEw==\n-----END PRIVATE KEY-----", "callback_domain": "localhost:8090"}', role='SRCA' WHERE id = $1`, models.NexmoChannelID)

	// set our UUID generator
	uuids.SetGenerator(uuids.NewSeededGenerator(0))

	// set our attachment domain for testing
	config.Mailroom.AttachmentDomain = "mailroom.io"
	defer func() { config.Mailroom.AttachmentDomain = "" }()

	oa, err := models.GetOrgAssets(ctx, db, models.Org1)
	assert.NoError(t, err)

	channel := oa.ChannelByUUID(models.NexmoChannelUUID)
	assert.NotNil(t, channel)

	c, err := NewClientFromChannel(http.DefaultClient, channel)
	assert.NoError(t, err)

	client := c.(*client)

	indentMarshal = false

	tcs := []struct {
		Events   []flows.Event
		Wait     flows.ActivatedWait
		Expected string
	}{
		{
			[]flows.Event{events.NewIVRCreated(flows.NewMsgOut(urn, channelRef, "hello world", nil, nil, nil, flows.NilMsgTopic))},
			nil,
			`[{"action":"talk","text":"hello world"}]`,
		},
		{
			[]flows.Event{events.NewIVRCreated(flows.NewMsgOut(urn, channelRef, "hello world", []utils.Attachment{utils.Attachment("audio:/recordings/foo.wav")}, nil, nil, flows.NilMsgTopic))},
			nil,
			`[{"action":"stream","streamUrl":["/recordings/foo.wav"]}]`,
		},
		{
			[]flows.Event{events.NewIVRCreated(flows.NewMsgOut(urn, channelRef, "hello world", []utils.Attachment{utils.Attachment("audio:https://temba.io/recordings/foo.wav")}, nil, nil, flows.NilMsgTopic))},
			nil,
			`[{"action":"stream","streamUrl":["https://temba.io/recordings/foo.wav"]}]`,
		},
		{
			[]flows.Event{
				events.NewIVRCreated(flows.NewMsgOut(urn, channelRef, "hello world", nil, nil, nil, flows.NilMsgTopic)),
				events.NewIVRCreated(flows.NewMsgOut(urn, channelRef, "goodbye", nil, nil, nil, flows.NilMsgTopic)),
			},
			nil,
			`[{"action":"talk","text":"hello world"},{"action":"talk","text":"goodbye"}]`,
		},
		{
			[]flows.Event{events.NewIVRCreated(flows.NewMsgOut(urn, channelRef, "enter a number", nil, nil, nil, flows.NilMsgTopic))},
			waits.NewActivatedMsgWait(nil, hints.NewFixedDigitsHint(1)),
			`[{"action":"talk","text":"enter a number","bargeIn":true},{"action":"input","maxDigits":1,"submitOnHash":true,"timeOut":30,"eventUrl":["http://temba.io/resume?session=1\u0026wait_type=gather\u0026sig=OjsMUDhaBTUVLq1e6I4cM0SKYpk%3D"],"eventMethod":"POST"}]`,
		},
		{
			[]flows.Event{events.NewIVRCreated(flows.NewMsgOut(urn, channelRef, "enter a number, then press #", nil, nil, nil, flows.NilMsgTopic))},
			waits.NewActivatedMsgWait(nil, hints.NewTerminatedDigitsHint("#")),
			`[{"action":"talk","text":"enter a number, then press #","bargeIn":true},{"action":"input","maxDigits":20,"submitOnHash":true,"timeOut":30,"eventUrl":["http://temba.io/resume?session=1\u0026wait_type=gather\u0026sig=OjsMUDhaBTUVLq1e6I4cM0SKYpk%3D"],"eventMethod":"POST"}]`,
		},
		{
			[]flows.Event{events.NewIVRCreated(flows.NewMsgOut(urn, channelRef, "say something", nil, nil, nil, flows.NilMsgTopic))},
			waits.NewActivatedMsgWait(nil, hints.NewAudioHint()),
			`[{"action":"talk","text":"say something"},{"action":"record","endOnKey":"#","timeOut":600,"endOnSilence":5,"eventUrl":["http://temba.io/resume?session=1\u0026wait_type=recording_url\u0026recording_uuid=f3ede2d6-becc-4ea3-ae5e-88526a9f4a57\u0026sig=Am9z7fXyU3SPCZagkSpddZSi6xY%3D"],"eventMethod":"POST"},{"action":"input","submitOnHash":true,"timeOut":1,"eventUrl":["http://temba.io/resume?session=1\u0026wait_type=record\u0026recording_uuid=f3ede2d6-becc-4ea3-ae5e-88526a9f4a57\u0026sig=fX1RhjcJNN4xYaiojVYakaz5F%2Fk%3D"],"eventMethod":"POST"}]`,
		},
	}

	for i, tc := range tcs {
		response, err := client.responseForSprint(resumeURL, tc.Wait, tc.Events)
		assert.NoError(t, err, "%d: unexpected error")
		assert.Equal(t, tc.Expected, response, "%d: unexpected response", i)
	}
}
