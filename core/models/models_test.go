package models

import (
	"os"
	"testing"

	"github.com/nyaruka/mailroom/testsuite"
)

// Custom entry point so we can reset our database
func TestMain(m *testing.M) {
	testsuite.Reset()
	os.Exit(m.Run())
}
