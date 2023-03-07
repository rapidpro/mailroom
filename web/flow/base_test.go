package flow_test

import (
	"testing"

	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
)

func TestServer(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	testsuite.RunWebTests(t, ctx, rt, "testdata/change_language.json", nil)
	testsuite.RunWebTests(t, ctx, rt, "testdata/clone.json", nil)
	testsuite.RunWebTests(t, ctx, rt, "testdata/inspect.json", nil)
	testsuite.RunWebTests(t, ctx, rt, "testdata/migrate.json", nil)
}

func TestPreviewStart(t *testing.T) {
	ctx, rt, mocks, close := testsuite.RuntimeWithSearch()
	defer close()

	mocks.ES.AddResponse(testdata.Cathy.ID)
	mocks.ES.AddResponse(testdata.Bob.ID)
	mocks.ES.AddResponse(testdata.George.ID)
	mocks.ES.AddResponse(testdata.Alexandria.ID)

	testsuite.RunWebTests(t, ctx, rt, "testdata/preview_start.json", nil)
}
