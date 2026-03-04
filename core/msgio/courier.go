package msgio

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	valkey "github.com/gomodule/redigo/redis"
	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/gocommon/i18n"
	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/utils/clogs"
)

var courierHttpClient = &http.Client{
	Timeout: 1 * time.Minute, // big so we let courier determine when things timeout
}

const (
	bulkPriority = 0
	highPriority = 1
)

type MsgOrigin string

const (
	MsgOriginFlow      MsgOrigin = "flow"
	MsgOriginBroadcast MsgOrigin = "broadcast"
	MsgOriginTicket    MsgOrigin = "ticket"
	MsgOriginChat      MsgOrigin = "chat"
)

type Contact struct {
	ID         models.ContactID  `json:"id"`
	UUID       flows.ContactUUID `json:"uuid"`
	LastSeenOn *time.Time        `json:"last_seen_on,omitempty"`
}

type OptInRef struct {
	ID   models.OptInID `json:"id"`
	Name string         `json:"name"`
}

type FlowRef struct {
	UUID assets.FlowUUID `json:"uuid"`
	Name string          `json:"name"`
}

type Templating struct {
	*flows.MsgTemplating
	Namespace  string `json:"namespace"`
	ExternalID string `json:"external_id"`
	Language   string `json:"language"`
}

type Session struct {
	UUID       flows.SessionUUID `json:"uuid"`
	Status     string            `json:"status"`
	SprintUUID flows.SprintUUID  `json:"sprint_uuid"`
	Timeout    int               `json:"timeout,omitempty"`
}

var sessionStatusMap = map[flows.SessionStatus]string{flows.SessionStatusWaiting: "W", flows.SessionStatusCompleted: "C"}

// Msg is the format of a message queued to courier
type Msg struct {
	UUID                 flows.EventUUID    `json:"uuid"`
	OrgID                models.OrgID       `json:"org_id"`
	Contact              *Contact           `json:"contact"`
	Origin               MsgOrigin          `json:"origin"`
	Text                 string             `json:"text"`
	Attachments          []utils.Attachment `json:"attachments,omitempty"`
	QuickReplies         []flows.QuickReply `json:"quick_replies,omitempty"`
	Locale               i18n.Locale        `json:"locale,omitempty"`
	Templating           *Templating        `json:"templating,omitempty"`
	HighPriority         bool               `json:"high_priority"`
	MsgCount             int                `json:"tps_cost"`
	CreatedOn            time.Time          `json:"created_on"`
	ChannelUUID          assets.ChannelUUID `json:"channel_uuid"`
	URN                  urns.URN           `json:"urn"`
	URNAuth              string             `json:"urn_auth,omitempty"`
	Flow                 *FlowRef           `json:"flow,omitempty"`
	UserID               models.UserID      `json:"user_id,omitempty"`
	OptIn                *OptInRef          `json:"optin,omitempty"`
	ResponseToExternalID string             `json:"response_to_external_id,omitempty"`
	IsResend             bool               `json:"is_resend,omitempty"`
	PrevAttempts         int                `json:"prev_attempts,omitempty"`
	Session              *Session           `json:"session,omitempty"`
}

// NewCourierMsg creates a courier message in the format it's expecting to be queued
func NewCourierMsg(oa *models.OrgAssets, mo *models.MsgOut, ch *models.Channel) (*Msg, error) {
	msg := &Msg{
		UUID:  mo.UUID(),
		OrgID: mo.OrgID(),
		Contact: &Contact{
			ID:         mo.ContactID(),
			UUID:       mo.Contact.UUID(),
			LastSeenOn: mo.Contact.LastSeenOn(),
		},
		Text:         mo.Text(),
		Attachments:  mo.Attachments(),
		QuickReplies: mo.QuickReplies(),
		Locale:       mo.Locale(),
		HighPriority: mo.HighPriority(),
		MsgCount:     mo.MsgCount(),
		CreatedOn:    mo.CreatedOn().In(time.UTC),
		ChannelUUID:  ch.UUID(),
		UserID:       mo.CreatedByID(),
		URN:          mo.URN.Identity,
		URNAuth:      string(mo.URN.AuthTokens["default"]),
		IsResend:     mo.IsResend,
		PrevAttempts: mo.ErrorCount(),
	}

	if mo.FlowID() != models.NilFlowID {
		msg.Origin = MsgOriginFlow
		flow, _ := oa.FlowByID(mo.FlowID())
		if flow != nil { // always a chance flow no longer exists
			msg.Flow = &FlowRef{UUID: flow.UUID(), Name: flow.Name()}
		}
	} else if mo.BroadcastID() != models.NilBroadcastID {
		msg.Origin = MsgOriginBroadcast
	} else if mo.TicketUUID() != "" {
		msg.Origin = MsgOriginTicket
	} else {
		msg.Origin = MsgOriginChat
	}

	if mo.Type() == models.MsgTypeOptIn {
		// this is an optin request
		optIn := oa.OptInByID(mo.OptInID())
		if optIn != nil {
			msg.OptIn = &OptInRef{ID: optIn.ID(), Name: optIn.Name()}
		}
	} else if mo.OptInID() != models.NilOptInID {
		// an optin on a broadcast message means use it for authentication
		msg.URNAuth = mo.URN.AuthTokens[fmt.Sprintf("optin:%d", mo.OptInID())]
	}

	if mo.Templating() != nil {
		tpl := oa.TemplateByUUID(mo.Templating().Template.UUID)
		if tpl != nil {
			tt := tpl.FindTranslation(ch, mo.Locale())
			if tt != nil {
				msg.Templating = &Templating{
					MsgTemplating: mo.Templating().MsgTemplating,
					Namespace:     tt.Namespace(),
					ExternalID:    tt.ExternalID(),
					Language:      tt.ExternalLocale(), // i.e. en_US
				}
			}
		}
	}

	if mo.ReplyTo != nil {
		msg.ResponseToExternalID = mo.ReplyTo.ExtID
	}
	if mo.Session != nil {
		msg.Session = &Session{
			UUID:       mo.Session.UUID(),
			Status:     sessionStatusMap[mo.Session.Status()],
			SprintUUID: mo.SprintUUID,
		}

		if mo.LastInSprint && mo.WaitTimeout != 0 {
			// This field is set on the last outgoing message in a session's sprint. In the case
			// of the session being at a wait with a timeout then the timeout will be set. It is up to
			// Courier to update the session's timeout appropriately after sending the message.
			msg.Session.Timeout = int(mo.WaitTimeout / time.Second)
		}
	}

	return msg, nil
}

var queuePushScript = valkey.NewScript(6, `
-- KEYS: [QueueType, QueueName, TPS, Priority, Items, EpochSecs]
local queueType, queueName, tps, priority, items, epochSecs = KEYS[1], KEYS[2], tonumber(KEYS[3]), KEYS[4], KEYS[5], KEYS[6]

-- first construct the base key for this queue from the type + name + tps, e.g. "msgs:0a77a158-1dcb-4c06-9aee-e15bdf64653e|10"
local queueKey = queueType .. ":" .. queueName .. "|" .. tps

-- each queue than has two sorted sets for bulk and high priority items, e.g. "msgs:0a77..653e|10/0" vs msgs:0a77..653e|10/1"
local priorityQueueKey = queueKey .. "/" .. priority

-- add the items to the sorted set using the full timestamp (e.g. 1636556789.123456) as the score
redis.call("ZADD", priorityQueueKey, epochSecs, items)

-- if we have a TPS limit, check the transaction counter for this epoch second to see if have already reached it
local curr = -1
if tps > 0 then
  local tpsKey = queueKey .. ":tps:" .. math.floor(epochSecs) -- e.g. "msgs:0a77..4653e|10:tps:1636556789"
  curr = tonumber(redis.call("GET", tpsKey))
end

-- if we haven't hit the limit, add this queue to set of active queues 
if not curr or curr < tps then
  redis.call("ZINCRBY", queueType .. ":active", 0, queueKey)
  return 1
else
  return 0
end
`)

// PushCourierBatch pushes a batch of messages for a single contact and channel onto the appropriate courier queue
func PushCourierBatch(vc valkey.Conn, oa *models.OrgAssets, ch *models.Channel, msgs []*models.MsgOut, timestamp string) error {
	priority := bulkPriority
	if msgs[0].HighPriority() {
		priority = highPriority
	}

	batch := make([]*Msg, len(msgs))
	for i, s := range msgs {
		var err error
		batch[i], err = NewCourierMsg(oa, s, ch)
		if err != nil {
			return fmt.Errorf("error creating courier message: %w", err)
		}
	}

	batchJSON := jsonx.MustMarshal(batch)

	_, err := queuePushScript.Do(vc, "msgs", ch.UUID(), ch.TPS(), priority, batchJSON, timestamp)
	return err
}

// QueueCourierMessages queues messages for a single contact to Courier
func QueueCourierMessages(vc valkey.Conn, oa *models.OrgAssets, contactID models.ContactID, channel *models.Channel, msgs []*models.MsgOut) error {
	if len(msgs) == 0 {
		return nil
	}

	// get the time in seconds since the epoch as a floating point number
	// e.g. 2021-11-10T15:10:49.123456+00:00 => "1636557205.123456"
	now := dates.Now()
	epochSeconds := strconv.FormatFloat(float64(now.UnixNano()/int64(time.Microsecond))/float64(1000000), 'f', 6, 64)

	// we batch msgs by priority
	batch := make([]*models.MsgOut, 0, len(msgs))

	currentPriority := msgs[0].HighPriority()

	// commits our batch to redis
	commitBatch := func() error {
		if len(batch) > 0 {
			start := time.Now()
			err := PushCourierBatch(vc, oa, channel, batch, epochSeconds)
			if err != nil {
				return err
			}
			slog.Debug("msgs queued to courier", "msgs", len(batch), "contact_id", contactID, "channel_uuid", channel.UUID(), "elapsed", time.Since(start))
		}
		return nil
	}

	for _, m := range msgs {
		// sanity check the state of the msg we're about to queue...
		assert(m.URN != nil, "can't queue a message to courier without a URN")
		assert(m.ContactURNID() != models.NilURNID, "can't queue a message to courier without a URNID")

		// if this msg is the same priority, add to current batch, otherwise start new batch
		if m.HighPriority() == currentPriority {
			batch = append(batch, m)
		} else {
			if err := commitBatch(); err != nil {
				return err
			}

			currentPriority = m.HighPriority()
			batch = []*models.MsgOut{m}
		}
	}

	// any remaining in our batch, queue it up
	return commitBatch()
}

var queueClearScript = valkey.NewScript(3, `
-- KEYS: [QueueType, QueueName, TPS]
local queueType, queueName, tps = KEYS[1], KEYS[2], tonumber(KEYS[3])

-- first construct the base key for this queue from the type + name + tps, e.g. "msgs:0a77a158-1dcb-4c06-9aee-e15bdf64653e|10"
local queueKey = queueType .. ":" .. queueName .. "|" .. tps

-- clear the sorted sets for the key
redis.call("DEL", queueKey .. "/1")
redis.call("DEL", queueKey .. "/0")

-- reset queue to zero
redis.call("ZADD", queueType .. ":active", 0, queueKey)
`)

// ClearCourierQueues clears the courier queues (priority and bulk) for the given channel
func ClearCourierQueues(vc valkey.Conn, ch *models.Channel) error {
	_, err := queueClearScript.Do(vc, "msgs", ch.UUID(), ch.TPS())
	return err
}

// see https://github.com/nyaruka/courier/blob/main/attachments.go#L23
type fetchAttachmentRequest struct {
	ChannelType models.ChannelType `json:"channel_type"`
	ChannelUUID assets.ChannelUUID `json:"channel_uuid"`
	URL         string             `json:"url"`
	MsgUUID     flows.EventUUID    `json:"msg_uuid"`
}

type fetchAttachmentResponse struct {
	Attachment struct {
		ContentType string `json:"content_type"`
		URL         string `json:"url"`
		Size        int    `json:"size"`
	} `json:"attachment"`
	LogUUID clogs.UUID `json:"log_uuid"`
}

// FetchAttachment calls courier to fetch the given attachment
func FetchAttachment(ctx context.Context, rt *runtime.Runtime, ch *models.Channel, attURL string, msgUUID flows.EventUUID) (utils.Attachment, clogs.UUID, error) {
	payload := jsonx.MustMarshal(&fetchAttachmentRequest{
		ChannelType: ch.Type(),
		ChannelUUID: ch.UUID(),
		URL:         attURL,
		MsgUUID:     msgUUID,
	})
	req, _ := http.NewRequest("POST", fmt.Sprintf("https://%s/c/_fetch-attachment", rt.Config.Domain), bytes.NewReader(payload))
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", rt.Config.CourierAuthToken))

	resp, err := httpx.DoTrace(courierHttpClient, req, nil, nil, -1)
	if err != nil {
		return "", "", fmt.Errorf("error calling courier endpoint: %w", err)
	}
	if resp.Response.StatusCode != 200 {
		return "", "", fmt.Errorf("error calling courier endpoint, got non-200 status: %s", string(resp.ResponseTrace))
	}
	fa := &fetchAttachmentResponse{}
	if err := json.Unmarshal(resp.ResponseBody, fa); err != nil {
		return "", "", fmt.Errorf("error unmarshaling courier response: %w", err)
	}

	return utils.Attachment(fmt.Sprintf("%s:%s", fa.Attachment.ContentType, fa.Attachment.URL)), fa.LogUUID, nil
}
