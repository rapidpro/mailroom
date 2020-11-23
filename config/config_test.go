package config_test

import (
	"net"
	"testing"

	"github.com/nyaruka/mailroom/config"

	"github.com/stretchr/testify/assert"
)

func TestParseDisallowedNetworks(t *testing.T) {
	cfg := config.NewMailroomConfig()

	privateNetwork1 := &net.IPNet{IP: net.IPv4(10, 0, 0, 0).To4(), Mask: net.CIDRMask(8, 32)}
	privateNetwork2 := &net.IPNet{IP: net.IPv4(172, 16, 0, 0).To4(), Mask: net.CIDRMask(12, 32)}
	privateNetwork3 := &net.IPNet{IP: net.IPv4(192, 168, 0, 0).To4(), Mask: net.CIDRMask(16, 32)}

	// test with config defaults
	ips, ipNets, err := cfg.ParseDisallowedNetworks()
	assert.NoError(t, err)
	assert.Equal(t, []net.IP{net.IPv4(127, 0, 0, 1), net.ParseIP(`::1`)}, ips)
	assert.Equal(t, []*net.IPNet{privateNetwork1, privateNetwork2, privateNetwork3}, ipNets)

	// test with empty
	cfg.DisallowedNetworks = ``
	ips, ipNets, err = cfg.ParseDisallowedNetworks()
	assert.NoError(t, err)
	assert.Equal(t, []net.IP{}, ips)
	assert.Equal(t, []*net.IPNet{}, ipNets)

	// test with invalid CSV
	cfg.DisallowedNetworks = `"127.0.0.1`
	_, _, err = cfg.ParseDisallowedNetworks()
	assert.EqualError(t, err, `record on line 1; parse error on line 2, column 0: extraneous or missing " in quoted-field`)

	// test with invalid IP
	cfg.DisallowedNetworks = `127.0.1`
	_, _, err = cfg.ParseDisallowedNetworks()
	assert.EqualError(t, err, `couldn't parse '127.0.1' as an IP address`)

	// test with invalid network
	cfg.DisallowedNetworks = `127.0.0.1/x`
	_, _, err = cfg.ParseDisallowedNetworks()
	assert.EqualError(t, err, `couldn't parse '127.0.0.1/x' as an IP network`)
}
