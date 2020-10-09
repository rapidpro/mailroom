package ivr

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/config"
	"github.com/nyaruka/mailroom/core/ivr"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/queue"
	"github.com/nyaruka/mailroom/core/tasks/starts"
	"github.com/pkg/errors"

	"github.com/nyaruka/mailroom/testsuite"
	"github.com/stretchr/testify/assert"
)

func TestIVR(t *testing.T) {
	ctx, db, rp := testsuite.Reset()
	models.FlushCache()

	rc := rp.Get()
	defer rc.Close()

	// register our mock client
	ivr.RegisterClientType(models.ChannelType("ZZ"), newMockClient)

	// update our twilio channel to be of type 'ZZ' and set max_concurrent_events to 1
	db.MustExec(`UPDATE channels_channel SET channel_type = 'ZZ', config = '{"max_concurrent_events": 1}' WHERE id = $1`, models.TwilioChannelID)

	// create a flow start for cathy
	start := models.NewFlowStart(models.Org1, models.StartTypeTrigger, models.IVRFlow, models.IVRFlowID, models.DoRestartParticipants, models.DoIncludeActive).
		WithContactIDs([]models.ContactID{models.CathyID})

	// call our master starter
	err := starts.CreateFlowBatches(ctx, db, rp, nil, start)
	assert.NoError(t, err)

	// should have one task in our ivr queue
	task, err := queue.PopNextTask(rc, queue.HandlerQueue)
	assert.NoError(t, err)
	batch := &models.FlowStartBatch{}
	err = json.Unmarshal(task.Task, batch)
	assert.NoError(t, err)

	client.callError = errors.Errorf("unable to create call")
	err = HandleFlowStartBatch(ctx, config.Mailroom, db, rp, batch)
	assert.NoError(t, err)
	testsuite.AssertQueryCount(t, db, `SELECT COUNT(*) FROM channels_channelconnection WHERE contact_id = $1 AND status = $2`, []interface{}{models.CathyID, models.ConnectionStatusFailed}, 1)

	client.callError = nil
	client.callID = ivr.CallID("call1")
	err = HandleFlowStartBatch(ctx, config.Mailroom, db, rp, batch)
	assert.NoError(t, err)
	testsuite.AssertQueryCount(t, db, `SELECT COUNT(*) FROM channels_channelconnection WHERE contact_id = $1 AND status = $2 AND external_id = $3`, []interface{}{models.CathyID, models.ConnectionStatusWired, "call1"}, 1)

	// trying again should put us in a throttled state (queued)
	client.callError = nil
	client.callID = ivr.CallID("call1")
	err = HandleFlowStartBatch(ctx, config.Mailroom, db, rp, batch)
	assert.NoError(t, err)
	testsuite.AssertQueryCount(t, db, `SELECT COUNT(*) FROM channels_channelconnection WHERE contact_id = $1 AND status = $2 AND next_attempt IS NOT NULL;`, []interface{}{models.CathyID, models.ConnectionStatusQueued}, 1)
}

var client = &MockClient{}

func newMockClient(httpClient *http.Client, channel *models.Channel) (ivr.Client, error) {
	return client, nil
}

type MockClient struct {
	callID    ivr.CallID
	callError error
}

func (c *MockClient) RequestCall(number urns.URN, handleURL string, statusURL string) (ivr.CallID, *httpx.Trace, error) {
	return c.callID, nil, c.callError
}

func (c *MockClient) HangupCall(externalID string) (*httpx.Trace, error) {
	return nil, nil
}

func (c *MockClient) WriteSessionResponse(session *models.Session, number urns.URN, resumeURL string, req *http.Request, w http.ResponseWriter) error {
	return nil
}

func (c *MockClient) WriteErrorResponse(w http.ResponseWriter, err error) error {
	return nil
}

func (c *MockClient) WriteEmptyResponse(w http.ResponseWriter, msg string) error {
	return nil
}

func (c *MockClient) InputForRequest(r *http.Request) (string, utils.Attachment, error) {
	return "", ivr.NilAttachment, nil
}

func (c *MockClient) StatusForRequest(r *http.Request) (models.ConnectionStatus, int) {
	return models.ConnectionStatusFailed, 10
}

func (c *MockClient) PreprocessResume(ctx context.Context, db *sqlx.DB, rp *redis.Pool, conn *models.ChannelConnection, r *http.Request) ([]byte, error) {
	return nil, nil
}

func (c *MockClient) ValidateRequestSignature(r *http.Request) error {
	return nil
}

func (c *MockClient) DownloadMedia(url string) (*http.Response, error) {
	return nil, nil
}

func (c *MockClient) URNForRequest(r *http.Request) (urns.URN, error) {
	return urns.NilURN, nil
}

func (c *MockClient) CallIDForRequest(r *http.Request) (string, error) {
	return "", nil
}
