package gsm7_test

import (
	"testing"

	"github.com/nyaruka/mailroom/utils/gsm7"

	"github.com/stretchr/testify/assert"
)

func TestSegments(t *testing.T) {
	// utility pads
	tenChars := "0123456789"
	unicodeTenChars := "☺123456789"
	extendedTenChars := "[123456789"
	fiftyChars := tenChars + tenChars + tenChars + tenChars + tenChars
	hundredChars := fiftyChars + fiftyChars
	unicode := "☺"

	tcs := []struct {
		Text     string
		Segments int
	}{
		{"", 1},
		{"hello", 1},
		{"“word”", 1},
		{hundredChars + fiftyChars + tenChars, 1},
		{hundredChars + fiftyChars + tenChars + "Z", 2},
		{hundredChars + fiftyChars + extendedTenChars, 2},
		{hundredChars + hundredChars + hundredChars + "123456", 2},
		{hundredChars + hundredChars + hundredChars + "1234567", 3},
		{fiftyChars + "zZ" + unicode, 1},
		{fiftyChars + tenChars + unicodeTenChars, 1},
		{fiftyChars + tenChars + unicodeTenChars + "z", 2},
	}

	for _, tc := range tcs {
		assert.Equal(t, tc.Segments, gsm7.Segments(tc.Text), "unexpected num of segments for: %s", tc.Text)
	}
}
