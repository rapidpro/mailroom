package ivr_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/mailroom/core/ivr"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/queue"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/core/tasks/starts"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestIVR(t *testing.T) {
	_, rt := testsuite.Runtime()
	rc := rt.RP.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetAll)

	// register our mock client
	ivr.RegisterServiceType(models.ChannelType("ZZ"), NewMockProvider)

	// update our twilio channel to be of type 'ZZ' and set max_concurrent_events to 1
	rt.DB.MustExec(`UPDATE channels_channel SET channel_type = 'ZZ', config = '{"max_concurrent_events": 1}' WHERE id = $1`, testdata.TwilioChannel.ID)

	// create a flow start for cathy
	start := models.NewFlowStart(testdata.Org1.ID, models.StartTypeTrigger, testdata.IVRFlow.ID).
		WithContactIDs([]models.ContactID{testdata.Cathy.ID})

	service.callError = errors.Errorf("unable to create call")

	err := tasks.Queue(rc, queue.BatchQueue, testdata.Org1.ID, &starts.StartFlowTask{FlowStart: start}, queue.DefaultPriority)
	require.NoError(t, err)

	testsuite.FlushTasks(t, rt)

	// should have one call in a failed state
	assertdb.Query(t, rt.DB, `SELECT COUNT(*) FROM ivr_call WHERE contact_id = $1 AND status = $2`, testdata.Cathy.ID, models.CallStatusFailed).Returns(1)

	// re-queue the start and try again
	service.callError = nil
	service.callID = ivr.CallID("call1")

	err = tasks.Queue(rc, queue.BatchQueue, testdata.Org1.ID, &starts.StartFlowTask{FlowStart: start}, queue.DefaultPriority)
	require.NoError(t, err)

	testsuite.FlushTasks(t, rt)

	assertdb.Query(t, rt.DB, `SELECT COUNT(*) FROM ivr_call WHERE contact_id = $1 AND status = $2 AND external_id = $3`, testdata.Cathy.ID, models.CallStatusWired, "call1").Returns(1)

	// trying again should put us in a throttled state (queued)
	service.callError = nil
	service.callID = ivr.CallID("call1")

	err = tasks.Queue(rc, queue.BatchQueue, testdata.Org1.ID, &starts.StartFlowTask{FlowStart: start}, queue.DefaultPriority)
	require.NoError(t, err)

	testsuite.FlushTasks(t, rt)

	assertdb.Query(t, rt.DB, `SELECT COUNT(*) FROM ivr_call WHERE contact_id = $1 AND status = $2 AND next_attempt IS NOT NULL;`, testdata.Cathy.ID, models.CallStatusQueued).Returns(1)
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

func (s *MockService) WriteSessionResponse(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, channel *models.Channel, call *models.Call, session *models.Session, number urns.URN, resumeURL string, req *http.Request, w http.ResponseWriter) error {
	return nil
}

func (s *MockService) WriteRejectResponse(w http.ResponseWriter) error {
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

func (s *MockService) StatusForRequest(r *http.Request) (models.CallStatus, models.CallError, int) {
	return models.CallStatusFailed, models.CallErrorProvider, 10
}

func (s *MockService) CheckStartRequest(r *http.Request) models.CallError {
	return ""
}

func (s *MockService) PreprocessResume(ctx context.Context, rt *runtime.Runtime, call *models.Call, r *http.Request) ([]byte, error) {
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
