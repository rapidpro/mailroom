package zendesk

import (
	"context"
	"database/sql"
	"net/http"
	"strconv"
	"time"

	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/goflow/utils/uuids"
	"github.com/nyaruka/mailroom/courier"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/web"

	"github.com/pkg/errors"
)

func init() {
	web.RegisterJSONRoute(http.MethodPost, "/mr/ticket/zendesk/pull", handlePull)
	web.RegisterJSONRoute(http.MethodPost, "/mr/ticket/zendesk/channelback", handleChannelback)
	web.RegisterJSONRoute(http.MethodPost, "/mr/ticket/zendesk/event_callback", handleEventCallback)
}

type pullRequest struct {
	State    string `form:"state"`
	Metadata string `form:"metadata" validate:"required"`
}

type pullResponse struct {
	ExternalResources []*Message `json:"external_resources"`
	State             string     `json:"state"`
}

type PendingMsg struct {
	ContactUUID string           `db:"contact_uuid"`
	ContactID   models.ContactID `db:"contact_id"`
	ContactName string           `db:"contact_name"`
	ID          int64            `db:"id"`
	Text        string           `db:"text"`
	CreatedOn   time.Time        `db:"created_on"`
}

const selectPendingMsgs = `
SELECT
  c.uuid as contact_uuid,
  c.id as contact_id,
  c.name as contact_name,
  m.id as id,
  m.text as text,
  m.created_on as created_on
FROM
  msgs_msg m JOIN
  contacts_contact c
ON
  m.contact_id = c.id
WHERE
  c.is_active = TRUE AND
  m.org_id = $1 AND
  m.id > $2
`

func handlePull(ctx context.Context, s *web.Server, r *http.Request) (interface{}, int, error) {
	request := &pullRequest{}
	if err := web.DecodeAndValidateForm(request, r); err != nil {
		return errors.Wrapf(err, "error decoding form"), http.StatusBadRequest, nil
	}

	// decode our metadata
	metadata := &Metadata{}
	if err := utils.UnmarshalAndValidate([]byte(request.Metadata), metadata); err != nil {
		return errors.Wrapf(err, "error unmarshaling metadata"), http.StatusBadRequest, nil
	}

	// validate our token
	org, err := models.LookupOrgByToken(ctx, s.DB, metadata.Token)
	if err != nil {
		return errors.Wrapf(err, "invalid authentication token"), http.StatusUnauthorized, nil
	}

	// decode our state
	state := &State{}
	if request.State != "" {
		if err := utils.UnmarshalAndValidate([]byte(request.State), state); err != nil {
			return errors.Wrapf(err, "error unmarshaling state"), http.StatusBadRequest, nil
		}
	}

	// select our messages
	rows, err := s.DB.QueryxContext(s.CTX, selectPendingMsgs, org.ID, state.LastMessageID)
	if err != nil && err != sql.ErrNoRows {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "error querying for pending messages")
	}
	defer rows.Close()

	response := &pullResponse{
		State:             "",
		ExternalResources: make([]*Message, 0),
	}

	if rows != nil {
		for rows.Next() {
			msg := &PendingMsg{}
			err := rows.StructScan(msg)
			if err != nil {
				return nil, http.StatusInternalServerError, errors.Wrapf(err, "error reading pending message row")
			}

			response.ExternalResources = append(
				response.ExternalResources,
				&Message{
					ExternalID: strconv.FormatInt(msg.ID, 10),
					Message:    msg.Text,
					CreatedAt:  msg.CreatedOn,
					Author: Author{
						ExternalID: strconv.FormatInt(int64(msg.ContactID), 10),
						Name:       msg.ContactName,
					},
					ThreadID:         string(msg.ContactUUID),
					AllowChannelBack: true,
				},
			)
		}
	}

	// get the latest messages
	return response, http.StatusOK, nil
}

type Metadata struct {
	OrgUUID uuids.UUID `json:"org_uuid"`
	Token   string     `json:"token"`
}

type State struct {
	LastMessageID int64 `json:"last_message_id"`
}

type Message struct {
	ExternalID string    `json:"external_id"`
	Message    string    `json:"message"`
	CreatedAt  time.Time `json:"created_at"`
	Author     Author    `json:"author"`

	ThreadID         string `json:"thread_id"`
	AllowChannelBack bool   `json:"allow_channelback"`
}

type channelbackRequest struct {
	Message     string   `form:"message" validate:"required"`
	FileURLs    []string `form:"file_urls"`
	ParentID    string   `form:"parent_id"`
	RecipientID string   `form:"recipient_id" validate:"required"`
	Metadata    string   `form:"metadata" validate:"required"`
}

type channelbackResponse struct {
	ExternalID       string `json:"external_id"`
	AllowChannelback bool   `json:"allow_channelback"`
}

func handleChannelback(ctx context.Context, s *web.Server, r *http.Request) (interface{}, int, error) {
	request := &channelbackRequest{}
	if err := web.DecodeAndValidateForm(request, r); err != nil {
		return errors.Wrapf(err, "error decoding form"), http.StatusBadRequest, nil
	}

	// decode our metadata
	metadata := &Metadata{}
	if err := utils.UnmarshalAndValidate([]byte(request.Metadata), metadata); err != nil {
		return errors.Wrapf(err, "error unmarshaling metadata"), http.StatusBadRequest, nil
	}

	// validate our token
	org, err := models.LookupOrgByToken(ctx, s.DB, metadata.Token)
	if err != nil {
		return errors.Wrapf(err, "invalid authentication token"), http.StatusUnauthorized, nil
	}

	// we build a simple translation
	translations := map[envs.Language]*models.BroadcastTranslation{
		envs.Language(""): {Text: request.Message},
	}

	// look up our assets
	assets, err := models.GetOrgAssets(s.CTX, s.DB, org.ID)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "error looking up org: %d", org.ID)
	}

	cid, err := strconv.Atoi(request.RecipientID)
	if err != nil {
		return errors.Wrapf(err, "invalid contact id: %s", request.RecipientID), http.StatusBadRequest, nil
	}

	// we'll use a broadcast to send this message
	bcast := models.NewBroadcast(org.ID, models.NilBroadcastID, translations, models.TemplateStateEvaluated, envs.Language(""), nil, nil, nil)
	batch := bcast.CreateBatch([]models.ContactID{models.ContactID(cid)})
	msgs, err := models.CreateBroadcastMessages(s.CTX, s.DB, s.RP, assets, batch)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "error creating message batch")
	}

	// queue our message
	rc := s.RP.Get()
	defer rc.Close()

	err = courier.QueueMessages(rc, msgs)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "error queuing outgoing message")
	}

	return &channelbackResponse{
		ExternalID:       strconv.FormatInt(int64(msgs[0].ID()), 10),
		AllowChannelback: true,
	}, http.StatusOK, nil
}

func handleEventCallback(ctx context.Context, s *web.Server, r *http.Request) (interface{}, int, error) {
	return map[string]string{"status": "OK"}, http.StatusOK, nil
}
