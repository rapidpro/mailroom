package flow_test

import (
	"testing"

	"github.com/nyaruka/mailroom/web"
)

func TestServer(t *testing.T) {
	web.RunWebTests(t, "testdata/change_language.json", nil)
	web.RunWebTests(t, "testdata/clone.json", nil)
	web.RunWebTests(t, "testdata/inspect.json", nil)
	web.RunWebTests(t, "testdata/migrate.json", nil)
}
