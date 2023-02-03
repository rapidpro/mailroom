package flow_test

import (
	"testing"

	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/nyaruka/mailroom/web"
)

func TestPreviewStart(t *testing.T) {
	ctx, rt, mocks, close := testsuite.Runtime()
	defer close()

	mocks.ES.AddResponse(testdata.Cathy.ID)
	mocks.ES.AddResponse(testdata.Bob.ID)
	mocks.ES.AddResponse(testdata.George.ID)
	mocks.ES.AddResponse(testdata.Alexandria.ID)

	web.RunWebTests(t, ctx, rt, "testdata/preview_start.json", nil)
}
