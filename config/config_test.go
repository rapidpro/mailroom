package config_test

import (
	"net"
	"testing"

	"github.com/nyaruka/mailroom/config"

	"github.com/stretchr/testify/assert"
)

func TestParseDisallowedIPs(t *testing.T) {
	cfg := config.NewMailroomConfig()

	// test with config defaults
	ips, err := cfg.ParseDisallowedIPs()
	assert.NoError(t, err)
	assert.Equal(t, []net.IP{net.IPv4(127, 0, 0, 1), net.ParseIP(`::1`)}, ips)

	// test with empty
	cfg.DisallowedIPs = ``
	ips, err = cfg.ParseDisallowedIPs()
	assert.NoError(t, err)
	assert.Equal(t, []net.IP{}, ips)

	// test with invalid CSV
	cfg.DisallowedIPs = `"127.0.0.1`
	_, err = cfg.ParseDisallowedIPs()
	assert.EqualError(t, err, `record on line 1; parse error on line 2, column 0: extraneous or missing " in quoted-field`)

	// test with invalid IP
	cfg.DisallowedIPs = `127.0.1`
	_, err = cfg.ParseDisallowedIPs()
	assert.EqualError(t, err, `couldn't parse '127.0.1' as an IP address`)
}
