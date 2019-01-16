package ivr

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"

	"github.com/go-chi/chi/middleware"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/ivr"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/web"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/nyaruka/mailroom/config"
)

func init() {
	web.RegisterRoute(http.MethodPost, "/mr/ivr/handle", handleIVRRequest)
}

const (
	actionStart  = "start"
	actionResume = "resume"
	actionStatus = "status"
)

// IVRRequest is our form for what fields we expect in IVR callbacks
type IVRRequest struct {
	StartID      models.StartID      `form:"start"`
	ConnectionID models.ConnectionID `form:"connection" validate:"required"`
	Action       string              `form:"action"     validate:"required"`
	URN          urns.URN            `form:"urn"        validate:"required"`
}

// writeClientError is just a small utility method to write out a simple JSON error when we don't have a client yet
// to do it on our behalf
func writeClientError(w http.ResponseWriter, err error) error {
	w.Header().Set("Content-type", "application/json")
	w.WriteHeader(http.StatusBadRequest)
	response := map[string]string{
		"error": err.Error(),
	}
	serialized, err := json.Marshal(response)
	if err != nil {
		return errors.Wrapf(err, "error serializing error")
	}
	_, err = w.Write([]byte(serialized))
	return errors.Wrapf(err, "error writing error")
}

// handleIVRRequest handles all incoming IVR requests related to a flow (status is handled elsewhere however)
func handleIVRRequest(ctx context.Context, s *web.Server, r *http.Request, rawW http.ResponseWriter) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second*55)
	defer cancel()

	// dump our request
	requestTrace, err := httputil.DumpRequest(r, true)
	if err != nil {
		return errors.Wrapf(err, "error creating request trace")
	}

	// wrap our writer
	responseTrace := &bytes.Buffer{}
	w := middleware.NewWrapResponseWriter(rawW, r.ProtoMajor)
	w.Tee(responseTrace)

	start := time.Now()

	request := &IVRRequest{}
	if err := web.DecodeAndValidateForm(request, r); err != nil {
		return errors.Wrapf(err, "request failed validation")
	}

	// load our connection
	conn, err := models.LoadChannelConnection(ctx, s.DB, request.ConnectionID)
	if err != nil {
		return errors.Wrapf(err, "unable to load channel connection with id: %d", request.ConnectionID)
	}

	// create a channel log for this request and connection
	defer func() {
		desc := "IVR event handled"
		isError := false
		if w.Status() != http.StatusOK {
			desc = "IVR Error"
			isError = true
		}

		path := r.URL.RequestURI()
		proxyPath := r.Header.Get("X-Forwarded-Path")
		if proxyPath != "" {
			path = proxyPath
		}

		url := fmt.Sprintf("https://%s%s", r.Host, path)
		_, err := models.InsertChannelLog(
			ctx, s.DB, desc, isError,
			r.Method, url, requestTrace, w.Status(), responseTrace.Bytes(),
			start, time.Since(start),
			conn,
		)
		if err != nil {
			logrus.WithError(err).Error("error writing ivr channel log")
		}
	}()

	// load our org
	org, err := models.GetOrgAssets(ctx, s.DB, conn.OrgID())
	if err != nil {
		return writeClientError(w, errors.Wrapf(err, "error loading org assets"))
	}

	// and our channel
	channel := org.ChannelByID(conn.ChannelID())
	if channel == nil {
		return writeClientError(w, errors.Wrapf(err, "no active channel with id: %d", conn.ChannelID()))
	}

	// get the right kind of client
	client, err := ivr.GetClient(channel)
	if client == nil {
		return writeClientError(w, errors.Wrapf(err, "unable to load client for channel: %d", conn.ChannelID()))
	}

	// validate this request's signature if relevant
	err = client.ValidateRequestSignature(r)
	if err != nil {
		return writeClientError(w, errors.Wrapf(err, "request failed signature validation"))
	}

	// load our contact
	contacts, err := models.LoadContacts(ctx, s.DB, org, []flows.ContactID{conn.ContactID()})
	if err != nil {
		return client.WriteErrorResponse(w, errors.Wrapf(err, "no such contact"))
	}
	if len(contacts) == 0 {
		return client.WriteErrorResponse(w, errors.Errorf("no contact width id: %d", conn.ContactID()))
	}
	if contacts[0].IsStopped() || contacts[0].IsBlocked() {
		return client.WriteErrorResponse(w, errors.Errorf("no contact width id: %d", conn.ContactID()))
	}

	// make sure our URN is indeed present on our contact, no funny business
	found := false
	for _, u := range contacts[0].URNs() {
		if u.Identity() == request.URN.Identity() {
			found = true
		}
	}
	if !found {
		return client.WriteErrorResponse(w, errors.Errorf("unable to find URN: %s on contact: %d", request.URN, conn.ContactID()))
	}

	domain := channel.ConfigValue(models.ChannelConfigCallbackDomain, config.Mailroom.Domain)
	form := url.Values{
		"action":     []string{actionResume},
		"connection": []string{fmt.Sprintf("%d", request.ConnectionID)},
		"urn":        []string{request.URN.String()},
	}

	resumeURL := fmt.Sprintf("https://%s/mr/ivr/handle?%s", domain, form.Encode())

	// if this a start, start our contact
	switch request.Action {
	case actionStart:
		err = ivr.StartIVRFlow(
			ctx, s.DB, s.RP, client, resumeURL,
			org, channel, conn, contacts[0], request.URN, request.StartID,
			r, w,
		)

	case actionResume:
		err = ivr.ResumeIVRFlow(
			ctx, s.Config, s.DB, s.RP, s.S3Client, resumeURL, client,
			org, channel, conn, contacts[0], request.URN,
			r, w,
		)

	case actionStatus:
		err = ivr.HandleIVRStatus(
			ctx, s.DB, s.RP, client, conn,
			r, w,
		)

	default:
		err = client.WriteErrorResponse(w, errors.Errorf("unknown action: %s", request.Action))
	}

	// had an error? mark our connection as errored and log it
	if err != nil {
		logrus.WithError(err).Error("error while handling IVR")
		return ivr.WriteErrorResponse(ctx, s.DB, client, conn, w, err)
	}

	return nil
}
