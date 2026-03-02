package system_test

import (
	"testing"

	"github.com/nyaruka/mailroom/testsuite"
)

func TestErrors(t *testing.T) {
	_, rt := testsuite.Runtime(t)

	testsuite.RunWebTests(t, rt, "testdata/errors.json")
}

func TestLatency(t *testing.T) {
	_, rt := testsuite.Runtime(t)

	testsuite.RunWebTests(t, rt, "testdata/latency.json")
}

func TestQueues(t *testing.T) {
	_, rt := testsuite.Runtime(t)

	testsuite.RunWebTests(t, rt, "testdata/queues.json")
}
