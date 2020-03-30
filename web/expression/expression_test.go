package expression_test

import (
	"testing"

	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/web"
)

func TestServer(t *testing.T) {
	testsuite.Reset()
	web.RunWebTests(t, "testdata/migrate.json")
}
