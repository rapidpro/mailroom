package zendesk

import (
	"context"
	"database/sql"
	"encoding/json"
	"html/template"
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
	web.RegisterJSONRoute(http.MethodPost, "/mr/ticket/zendesk/pull", web.RequireAuthToken(handlePull))
	web.RegisterJSONRoute(http.MethodPost, "/mr/ticketzendesk/channelback", web.RequireAuthToken(handleChannelback))
	web.RegisterJSONRoute(http.MethodPost, "/mr/ticket/zendesk/event_callback", web.RequireAuthToken(handleEventCallback))
	web.RegisterJSONRoute(http.MethodGet, "/mr/ticket/zendesk/manifest", handleManifest)
	web.RegisterRoute(http.MethodGet, "/mr/ticket/zendesk/admin_ui", handleAdmin)
	web.RegisterRoute(http.MethodPost, "/mr/ticket/zendesk/admin_ui", handleAdmin)
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
	org, err := models.LookupOrgByToken(ctx, s.DB, metadata.OrgUUID, "Prometheus", metadata.Token)
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

type Author struct {
	ExternalID string `json:"external_id"`
	Name       string `json:"name"`
	// Locale string `json:"locale"`
	// Fields map[string]string `json:"fields"`
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
	org, err := models.LookupOrgByToken(ctx, s.DB, metadata.OrgUUID, "Prometheus", metadata.Token)
	if err != nil {
		return errors.Wrapf(err, "invalid authentication token"), http.StatusUnauthorized, nil
	}

	// we build a simple translation
	translations := map[envs.Language]*models.BroadcastTranslation{
		envs.Language(""): &models.BroadcastTranslation{Text: request.Message},
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

func handleManifest(ctx context.Context, s *web.Server, r *http.Request) (interface{}, int, error) {
	return struct {
		Name             string            `json:"name"`
		ID               string            `json:"id"`
		Author           string            `json:"author"`
		Version          string            `json:"version"`
		ChannelbackFiles bool              `json:"channelback_files"`
		URLs             map[string]string `json:"urls"`
	}{
		Name:             "RapidPro",
		Author:           "Nyaruka",
		ID:               "com.nyaruka.rapidpro.zendesk",
		Version:          "v0.0.1",
		ChannelbackFiles: false,
		URLs: map[string]string{
			"admin_ui":           "./admin_ui",
			"pull_url":           "./pull",
			"channelback_url":    "./channelback",
			"event_callback_url": "./event_callback",
		},
	}, http.StatusOK, nil
}

func renderAdminForm(w http.ResponseWriter, form map[string]string) error {
	err := authForm.Execute(w, form)
	if err != nil {
		return errors.Wrapf(err, "error executing template")
	}

	return nil
}

func handleAdmin(ctx context.Context, s *web.Server, r *http.Request, w http.ResponseWriter) error {
	err := r.ParseForm()
	if err != nil {
		return errors.Wrapf(err, "error parsing form")
	}
	orgUUID := r.Form.Get("username")
	token := r.Form.Get("token")
	returnURL := r.Form.Get("return_url")

	if returnURL == "" {
		return renderAdminForm(w, map[string]string{
			"error": "Missing return URL",
		})
	}

	// empty form, just render the form
	if orgUUID == "" && token == "" {
		return renderAdminForm(w, map[string]string{
			"return_url": r.Form.Get("return_url"),
		})
	}

	// otherwise try to validate the token
	org, err := models.LookupOrgByToken(ctx, s.DB, uuids.UUID(orgUUID), "Prometheus", token)
	if err != nil || org == nil {
		return renderAdminForm(w, map[string]string{
			"error":      "Invalid username or password",
			"username":   orgUUID,
			"token":      token,
			"return_url": r.Form.Get("return_url"),
		})
	}

	metadata := Metadata{
		OrgUUID: org.UUID,
		Token:   token,
	}
	jdata, _ := json.Marshal(metadata)

	err = authSuccess.Execute(w, map[string]string{
		"name":       org.Name,
		"metadata":   string(jdata),
		"return_url": returnURL,
	})
	if err != nil {
		return errors.Wrapf(err, "error executing template")
	}

	return nil
}

const authFormHTML = `
<!DOCTYPE html>
	<html lang="en">
	<head>
		<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@zendeskgarden/css-buttons@7.0.19/dist/index.min.css">
		<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@zendeskgarden/css-forms@7.0.20/dist/index.min.css">
		<meta charset="utf-8">
		<style>
			body { 
				font-family: sans-serif; 
				font-size: 14px;
				color: rgb(85, 85, 85);
				max-width: 370px;
			}
			.c-txt__label {
				color: rgb(85, 85, 85);
			}
			.c-txt {
				margin-bottom: 15px;
			}
			.first {
				margin-top: 15px;
			}
		</style>
	</head>
	<body>
		<div class="preamble">
			This will connect your TextIt account with ZenDesk. You can find your ZenDesk integration username and
			password on your account page where you can enable this integration.
		</div>

		<small class="c-txt__message c-txt__message--error">{{.error}}</small>

		<form method="post" action = "./admin_ui">
		<div class="first c-txt u-mb-sm">
			<label class="c-txt__label" for="username">Username</label>
			<input class="c-txt__input" type="text" name="username" value="{{.username}}"></input>
			<small class="c-txt__message"><span dir="ltr">Your ZenDesk integration username</span></small>
		</div>

		<div class="c-txt u-mb-sm">
			<label class="c-txt__label" for="username">Password</label>
			<input class="c-txt__input" type="text" name="token" value="{{.token}}"></input>
			<small class="c-txt__message"><span dir="ltr">Your ZenDesk integration password</span></small>
		</div>		

   		<input type="hidden" name="return_url" value="{{.return_url}}"></input>
    	<input type="submit" class="c-btn u-mt-sm" value="Add Account">
		</form>
	</body>
	</html>
`

const authSuccessHTML = `
	<html><body>
		<form id="finish" method="post" action="{{.return_url}}">
  			<input type="hidden" name="name" value="{{.name}}"></input>
  			<input type="hidden" name="metadata" value="{{.metadata}}"></input>
		</form>
		<script type="text/javascript">
		var form = document.forms['finish'];
	  	form.submit();
		</script>
	</body></html>`

var authForm *template.Template
var authSuccess *template.Template

func init() {
	authForm = template.Must(template.New("form").Option("missingkey=zero").Parse(authFormHTML))
	authSuccess = template.Must(template.New("success").Option("missingkey=zero").Parse(authSuccessHTML))
}
