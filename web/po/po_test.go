package flow_test

import (
	"testing"

	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/web"
)

func TestServer(t *testing.T) {
	ctx, rt, _, _ := testsuite.Get()

	web.RunWebTests(t, ctx, rt, "testdata/export.json", nil)
	web.RunWebTests(t, ctx, rt, "testdata/import.json", nil)
}
