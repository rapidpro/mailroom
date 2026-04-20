package flow_test

import (
	"testing"

	"github.com/nyaruka/mailroom/testsuite"
)

func TestServer(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	testsuite.RunWebTests(t, ctx, rt, "testdata/change_language.json", nil)
	testsuite.RunWebTests(t, ctx, rt, "testdata/clone.json", nil)
	testsuite.RunWebTests(t, ctx, rt, "testdata/inspect.json", nil)
	testsuite.RunWebTests(t, ctx, rt, "testdata/migrate.json", nil)
}

func TestStartPreview(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	testsuite.RunWebTests(t, ctx, rt, "testdata/start_preview.json", nil)
}
