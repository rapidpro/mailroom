package runtime_test

import (
	"net"
	"testing"

	"github.com/nyaruka/mailroom/runtime"

	"github.com/stretchr/testify/assert"
)

func TestValidate(t *testing.T) {
	c := runtime.NewDefaultConfig()
	assert.NoError(t, c.Validate())

	c.DB = "??"
	c.ReadonlyDB = "??"
	c.Redis = "??"
	c.Elastic = "??"
	c.SessionStorage = "??"
	assert.EqualError(t, c.Validate(), "field 'DB' is not a valid URL, field 'ReadonlyDB' is not a valid URL, field 'Redis' is not a valid URL, field 'SessionStorage' is not a valid session storage mode, field 'Elastic' is not a valid URL")

	c = runtime.NewDefaultConfig()
	c.DB = "mysql://temba:temba@localhost/temba"
	c.Redis = "bluedis://localhost:6379/15"
	assert.EqualError(t, c.Validate(), "field 'DB' must start with 'postgres:', field 'Redis' must start with 'redis:'")
}

func TestParseDisallowedNetworks(t *testing.T) {
	cfg := runtime.NewDefaultConfig()

	privateNetwork1 := &net.IPNet{IP: net.IPv4(10, 0, 0, 0).To4(), Mask: net.CIDRMask(8, 32)}
	privateNetwork2 := &net.IPNet{IP: net.IPv4(172, 16, 0, 0).To4(), Mask: net.CIDRMask(12, 32)}
	privateNetwork3 := &net.IPNet{IP: net.IPv4(192, 168, 0, 0).To4(), Mask: net.CIDRMask(16, 32)}

	linkLocalIPv4 := &net.IPNet{IP: net.IPv4(169, 254, 0, 0).To4(), Mask: net.CIDRMask(16, 32)}
	_, linkLocalIPv6, _ := net.ParseCIDR("fe80::/10")

	// test with config defaults
	ips, ipNets, err := cfg.ParseDisallowedNetworks()
	assert.NoError(t, err)
	assert.Equal(t, []net.IP{net.IPv4(127, 0, 0, 1), net.ParseIP(`::1`)}, ips)
	assert.Equal(t, []*net.IPNet{privateNetwork1, privateNetwork2, privateNetwork3, linkLocalIPv4, linkLocalIPv6}, ipNets)

	// test with empty
	cfg.DisallowedNetworks = ``
	ips, ipNets, err = cfg.ParseDisallowedNetworks()
	assert.NoError(t, err)
	assert.Equal(t, []net.IP{}, ips)
	assert.Equal(t, []*net.IPNet{}, ipNets)

	// test with invalid CSV
	cfg.DisallowedNetworks = `"127.0.0.1`
	_, _, err = cfg.ParseDisallowedNetworks()
	assert.EqualError(t, err, `parse error on line 1, column 11: extraneous or missing " in quoted-field`)
}
