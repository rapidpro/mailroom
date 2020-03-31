package flow_test

import (
	"testing"

	"github.com/nyaruka/mailroom/web"
)

func TestServer(t *testing.T) {
	web.RunWebTests(t, "testdata/export.json")
	web.RunWebTests(t, "testdata/import.json")
}
