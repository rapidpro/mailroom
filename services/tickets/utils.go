package tickets

import (
	"context"
	"net/http"
	"path/filepath"
	"time"

	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/gocommon/storage"
	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/courier"
	"github.com/nyaruka/mailroom/models"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
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
func FromTicketUUID(ctx context.Context, db *sqlx.DB, uuid flows.TicketUUID, ticketerType string) (*models.Ticket, *models.Ticketer, models.TicketService, error) {
	// look up our ticket
	ticket, err := models.LookupTicketByUUID(ctx, db, uuid)
	if err != nil || ticket == nil {
		return nil, nil, nil, errors.Errorf("error looking up ticket %s", uuid)
	}

	// look up our assets
	assets, err := models.GetOrgAssets(ctx, db, ticket.OrgID())
	if err != nil {
		return nil, nil, nil, errors.Wrapf(err, "error looking up org #%d", ticket.OrgID())
	}

	// and get the ticketer for this ticket
	ticketer := assets.TicketerByID(ticket.TicketerID())
	if ticketer == nil || ticketer.Type() != ticketerType {
		return nil, nil, nil, errors.Errorf("error looking up ticketer #%d", ticket.TicketerID())
	}

	// and load it as a service
	svc, err := ticketer.AsService(flows.NewTicketer(ticketer))
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "error loading ticketer service")
	}

	return ticket, ticketer, svc, nil
}

// FromTicketerUUID takes a ticketer UUID and looks up the ticketer and creates the service
func FromTicketerUUID(ctx context.Context, db *sqlx.DB, uuid assets.TicketerUUID, ticketerType string) (*models.Ticketer, models.TicketService, error) {
	ticketer, err := models.LookupTicketerByUUID(ctx, db, uuid)
	if err != nil || ticketer == nil || ticketer.Type() != ticketerType {
		return nil, nil, errors.Errorf("error looking up ticketer %s", uuid)
	}

	// and load it as a service
	svc, err := ticketer.AsService(flows.NewTicketer(ticketer))
	if err != nil {
		return nil, nil, errors.Wrap(err, "error loading ticketer service")
	}

	return ticketer, svc, nil
}

// SendReply sends a message reply from the ticket system user to the contact
func SendReply(ctx context.Context, db *sqlx.DB, rp *redis.Pool, store storage.Storage, ticket *models.Ticket, text string, fileURLs []string) (*models.Msg, error) {
	// look up our assets
	oa, err := models.GetOrgAssets(ctx, db, ticket.OrgID())
	if err != nil {
		return nil, errors.Wrapf(err, "error looking up org #%d", ticket.OrgID())
	}

	// fetch and files and prepare as attachments
	attachments := make([]utils.Attachment, len(fileURLs))
	for i, fileURL := range fileURLs {
		fileBody, err := fetchFile(fileURL)
		if err != nil {
			return nil, errors.Wrapf(err, "error fetching file %s for ticket reply", fileURL)
		}

		filename := string(uuids.New()) + filepath.Ext(fileURL)

		attachments[i], err = oa.Org().StoreAttachment(store, filename, fileBody)
		if err != nil {
			return nil, errors.Wrapf(err, "error storing attachment %s for ticket reply", fileURL)
		}
	}

	// build a simple translation
	base := &models.BroadcastTranslation{Text: text, Attachments: attachments}
	translations := map[envs.Language]*models.BroadcastTranslation{envs.Language("base"): base}

	// we'll use a broadcast to send this message
	bcast := models.NewBroadcast(oa.OrgID(), models.NilBroadcastID, translations, models.TemplateStateEvaluated, envs.Language("base"), nil, nil, nil)
	batch := bcast.CreateBatch([]models.ContactID{ticket.ContactID()})
	msgs, err := models.CreateBroadcastMessages(ctx, db, rp, oa, batch)
	if err != nil {
		return nil, errors.Wrapf(err, "error creating message batch")
	}

	msg := msgs[0]

	// queue our message
	rc := rp.Get()
	defer rc.Close()

	err = courier.QueueMessages(rc, []*models.Msg{msg})
	if err != nil {
		return msg, errors.Wrapf(err, "error queuing ticket reply")
	}
	return msg, nil
}

func fetchFile(url string) ([]byte, error) {
	req, _ := httpx.NewRequest("GET", url, nil, nil)

	trace, err := httpx.DoTrace(http.DefaultClient, req, httpx.NewFixedRetries(time.Second*5, time.Second*10), nil, 10*1024*1024)
	if err != nil {
		return nil, err
	}
	if trace.Response.StatusCode/100 != 2 {
		return nil, errors.New("fetch returned non-200 response")
	}

	return trace.ResponseBody, nil
}
