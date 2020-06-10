package flow_test

import (
	"testing"

	"github.com/nyaruka/mailroom/web"
)

func TestServer(t *testing.T) {
	web.RunWebTests(t, "testdata/clone.json")
	web.RunWebTests(t, "testdata/inspect.json")
	web.RunWebTests(t, "testdata/migrate.json")
}
