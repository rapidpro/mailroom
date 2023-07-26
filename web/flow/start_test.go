package flow_test

import (
	"testing"

	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/nyaruka/mailroom/web"
)

func TestPreviewStart(t *testing.T) {
	ctx, rt, _, _ := testsuite.Get()

	mockES := testsuite.NewMockElasticServer()
	defer mockES.Close()

	rt.ES = mockES.Client()

	mockES.AddResponse(testdata.Cathy.ID)
	mockES.AddResponse(testdata.Bob.ID)
	mockES.AddResponse(testdata.George.ID)
	mockES.AddResponse(testdata.Alexandria.ID)

	web.RunWebTests(t, ctx, rt, "testdata/preview_start.json", nil)
}
