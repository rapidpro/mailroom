package ivr_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/mailroom/core/ivr"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/queue"
	ivrtasks "github.com/nyaruka/mailroom/core/tasks/ivr"
	"github.com/nyaruka/mailroom/core/tasks/starts"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestIVR(t *testing.T) {
	ctx, rt, db, rp := testsuite.Get()
	rc := rp.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetAll)

	// register our mock client
	ivr.RegisterServiceType(models.ChannelType("ZZ"), NewMockProvider)

	// update our twilio channel to be of type 'ZZ' and set max_concurrent_events to 1
	db.MustExec(`UPDATE channels_channel SET channel_type = 'ZZ', config = '{"max_concurrent_events": 1}' WHERE id = $1`, testdata.TwilioChannel.ID)

	// create a flow start for cathy
	start := models.NewFlowStart(testdata.Org1.ID, models.StartTypeTrigger, models.FlowTypeVoice, testdata.IVRFlow.ID).
		WithContactIDs([]models.ContactID{testdata.Cathy.ID})

	// call our master starter
	err := starts.CreateFlowBatches(ctx, rt, start)
	assert.NoError(t, err)

	// should have one task in our ivr queue
	task, err := queue.PopNextTask(rc, queue.HandlerQueue)
	assert.NoError(t, err)
	batch := &models.FlowStartBatch{}
	err = json.Unmarshal(task.Task, batch)
	assert.NoError(t, err)

	service.callError = errors.Errorf("unable to create call")
	err = ivrtasks.HandleFlowStartBatch(ctx, rt, batch)
	assert.NoError(t, err)
	assertdb.Query(t, db, `SELECT COUNT(*) FROM channels_channelconnection WHERE contact_id = $1 AND status = $2`, testdata.Cathy.ID, models.ConnectionStatusFailed).Returns(1)

	service.callError = nil
	service.callID = ivr.CallID("call1")
	err = ivrtasks.HandleFlowStartBatch(ctx, rt, batch)
	assert.NoError(t, err)
	assertdb.Query(t, db, `SELECT COUNT(*) FROM channels_channelconnection WHERE contact_id = $1 AND status = $2 AND external_id = $3`, testdata.Cathy.ID, models.ConnectionStatusWired, "call1").Returns(1)

	// trying again should put us in a throttled state (queued)
	service.callError = nil
	service.callID = ivr.CallID("call1")
	err = ivrtasks.HandleFlowStartBatch(ctx, rt, batch)
	assert.NoError(t, err)
	assertdb.Query(t, db, `SELECT COUNT(*) FROM channels_channelconnection WHERE contact_id = $1 AND status = $2 AND next_attempt IS NOT NULL;`, testdata.Cathy.ID, models.ConnectionStatusQueued).Returns(1)
}

var service = &MockService{}

func NewMockProvider(httpClient *http.Client, channel *models.Channel) (ivr.Service, error) {
	return service, nil
}

type MockService struct {
	callID    ivr.CallID
	callError error
}

func (s *MockService) RequestCall(number urns.URN, handleURL string, statusURL string, machineDetection bool) (ivr.CallID, *httpx.Trace, error) {
	return s.callID, nil, s.callError
}

func (s *MockService) HangupCall(externalID string) (*httpx.Trace, error) {
	return nil, nil
}

func (s *MockService) WriteSessionResponse(ctx context.Context, rt *runtime.Runtime, channel *models.Channel, conn *models.ChannelConnection, session *models.Session, number urns.URN, resumeURL string, req *http.Request, w http.ResponseWriter) error {
	return nil
}

func (s *MockService) WriteErrorResponse(w http.ResponseWriter, err error) error {
	return nil
}

func (s *MockService) WriteEmptyResponse(w http.ResponseWriter, msg string) error {
	return nil
}

func (s *MockService) ResumeForRequest(r *http.Request) (ivr.Resume, error) {
	return nil, nil
}

func (s *MockService) StatusForRequest(r *http.Request) (models.ConnectionStatus, models.ConnectionError, int) {
	return models.ConnectionStatusFailed, models.ConnectionErrorProvider, 10
}

func (s *MockService) CheckStartRequest(r *http.Request) models.ConnectionError {
	return ""
}

func (s *MockService) PreprocessResume(ctx context.Context, rt *runtime.Runtime, conn *models.ChannelConnection, r *http.Request) ([]byte, error) {
	return nil, nil
}

func (s *MockService) PreprocessStatus(ctx context.Context, rt *runtime.Runtime, r *http.Request) ([]byte, error) {
	return nil, nil
}

func (s *MockService) ValidateRequestSignature(r *http.Request) error {
	return nil
}

func (s *MockService) DownloadMedia(url string) (*http.Response, error) {
	return nil, nil
}

func (s *MockService) URNForRequest(r *http.Request) (urns.URN, error) {
	return urns.NilURN, nil
}

func (s *MockService) CallIDForRequest(r *http.Request) (string, error) {
	return "", nil
}

func (s *MockService) RedactValues(*models.Channel) []string {
	return []string{"sesame"}
}
