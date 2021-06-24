package msg_test

import (
	"testing"

	"github.com/nyaruka/mailroom/web"
)

func TestServer(t *testing.T) {
	web.RunWebTests(t, "testdata/resend.json", nil)
}
