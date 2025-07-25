package tickets

import (
	"bytes"
	"context"
	"io"
	"mime"
	"net/http"
	"path/filepath"
	"time"

	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/msgio"
	"github.com/nyaruka/mailroom/core/tasks/handler"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/pkg/errors"
)

// GetContactDisplay gets a non-empty display value for a contact for use on a ticket
func GetContactDisplay(env envs.Environment, contact *flows.Contact) string {
	display := contact.Format(env)
	if display == "" {
		return "Anonymous"
	}
	return display
}

// FromTicketUUID takes a ticket UUID and looks up the ticket and ticketer, and creates the service
func FromTicketUUID(ctx context.Context, rt *runtime.Runtime, uuid flows.TicketUUID, ticketerType string) (*models.Ticket, *models.Ticketer, models.TicketService, error) {
	// look up our ticket
	ticket, err := models.LookupTicketByUUID(ctx, rt.DB, uuid)
	if err != nil || ticket == nil {
		return nil, nil, nil, errors.Errorf("error looking up ticket %s", uuid)
	}

	// look up our assets
	assets, err := models.GetOrgAssets(ctx, rt, ticket.OrgID())
	if err != nil {
		return nil, nil, nil, errors.Wrapf(err, "error looking up org #%d", ticket.OrgID())
	}

	// and get the ticketer for this ticket
	ticketer := assets.TicketerByID(ticket.TicketerID())
	if ticketer == nil || ticketer.Type() != ticketerType {
		return nil, nil, nil, errors.Errorf("error looking up ticketer #%d", ticket.TicketerID())
	}

	// and load it as a service
	svc, err := ticketer.AsService(rt.Config, flows.NewTicketer(ticketer))
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "error loading ticketer service")
	}

	return ticket, ticketer, svc, nil
}

// FromTicketerUUID takes a ticketer UUID and looks up the ticketer and creates the service
func FromTicketerUUID(ctx context.Context, rt *runtime.Runtime, uuid assets.TicketerUUID, ticketerType string) (*models.Ticketer, models.TicketService, error) {
	ticketer, err := models.LookupTicketerByUUID(ctx, rt.DB, uuid)
	if err != nil || ticketer == nil || ticketer.Type() != ticketerType {
		return nil, nil, errors.Errorf("error looking up ticketer %s", uuid)
	}

	// and load it as a service
	svc, err := ticketer.AsService(rt.Config, flows.NewTicketer(ticketer))
	if err != nil {
		return nil, nil, errors.Wrap(err, "error loading ticketer service")
	}

	return ticketer, svc, nil
}

// SendReply sends a message reply from the ticket system user to the contact
func SendReply(ctx context.Context, rt *runtime.Runtime, ticket *models.Ticket, text string, files []*File) (*models.Msg, error) {
	// look up our assets
	oa, err := models.GetOrgAssets(ctx, rt, ticket.OrgID())
	if err != nil {
		return nil, errors.Wrapf(err, "error looking up org #%d", ticket.OrgID())
	}

	// load the contact and generate as a flow contact
	c, err := models.LoadContact(ctx, rt.DB, oa, ticket.ContactID())
	if err != nil {
		return nil, errors.Wrap(err, "error loading contact")
	}

	contact, err := c.FlowContact(oa)
	if err != nil {
		return nil, errors.Wrap(err, "error creating flow contact")
	}

	// upload files to create message attachments
	attachments := make([]utils.Attachment, len(files))
	for i, file := range files {
		filename := string(uuids.New()) + filepath.Ext(file.URL)

		attachments[i], err = oa.Org().StoreAttachment(ctx, rt, filename, file.ContentType, file.Body)
		if err != nil {
			return nil, errors.Wrapf(err, "error storing attachment %s for ticket reply", file.URL)
		}
	}

	out, ch := models.NewMsgOut(oa, contact, text, attachments, nil, contact.Locale(oa.Env()))
	msg, err := models.NewOutgoingTicketMsg(rt, oa.Org(), ch, contact, out, dates.Now(), ticket.ID(), models.NilUserID)
	if err != nil {
		return nil, errors.Wrap(err, "error creating outgoing message")
	}

	err = models.InsertMessages(ctx, rt.DB, []*models.Msg{msg})
	if err != nil {
		return nil, errors.Wrap(err, "error inserting outgoing message")
	}

	if err := models.RecordTicketReply(ctx, rt.DB, oa, ticket.ID(), models.NilUserID); err != nil {
		return nil, errors.Wrap(err, "error recording ticket reply")
	}

	msgio.SendMessages(ctx, rt, rt.DB, nil, []*models.Msg{msg})
	return msg, nil
}

var retries = httpx.NewFixedRetries(time.Second*5, time.Second*10)

// File represents a file sent to us from a ticketing service
type File struct {
	URL         string
	ContentType string
	Body        io.ReadCloser
}

// FetchFile fetches a file from the given URL
func FetchFile(url string, headers map[string]string) (*File, error) {
	req, _ := httpx.NewRequest("GET", url, nil, headers)

	trace, err := httpx.DoTrace(http.DefaultClient, req, retries, nil, 10*1024*1024)
	if err != nil {
		return nil, err
	}
	if trace.Response.StatusCode/100 != 2 {
		return nil, errors.New("fetch returned non-200 response")
	}

	contentType, _, _ := mime.ParseMediaType(trace.Response.Header.Get("Content-Type"))

	return &File{URL: url, ContentType: contentType, Body: io.NopCloser(bytes.NewReader(trace.ResponseBody))}, nil
}

// Close closes the given ticket, and creates and queues a closed event
func Close(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, ticket *models.Ticket, externally bool, l *models.HTTPLogger) error {
	events, err := models.CloseTickets(ctx, rt, oa, models.NilUserID, []*models.Ticket{ticket}, externally, false, l)
	if err != nil {
		return errors.Wrap(err, "error closing ticket")
	}

	if len(events) == 1 {
		rc := rt.RP.Get()
		defer rc.Close()

		err = handler.QueueTicketEvent(rc, ticket.ContactID(), events[ticket])
		if err != nil {
			return errors.Wrapf(err, "error queueing ticket closed event")
		}
	}

	return nil
}

// Reopen reopens the given ticket
func Reopen(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, ticket *models.Ticket, externally bool, l *models.HTTPLogger) error {
	_, err := models.ReopenTickets(ctx, rt, oa, models.NilUserID, []*models.Ticket{ticket}, externally, l)
	return err
}
