package expression_test

import (
	"testing"

	"github.com/nyaruka/mailroom/testsuite"
)

func TestServer(t *testing.T) {
	ctx, rt, _, _ := testsuite.Get()

	testsuite.RunWebTests(t, ctx, rt, "testdata/migrate.json", nil)
}
