package web

import (
	"testing"

	"github.com/nyaruka/mailroom/testsuite"
)

func TestServer(t *testing.T) {
	ctx, rt, _, _ := testsuite.Get()

	RunWebTests(t, ctx, rt, "testdata/server.json", nil)
}
