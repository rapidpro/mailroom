package web

import (
	"testing"
)

func TestServer(t *testing.T) {
	RunWebTests(t, "testdata/server.json", nil)
}
