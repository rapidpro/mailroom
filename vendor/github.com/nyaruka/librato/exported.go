package librato

import (
	"sync"
	"time"
)

// Collector is anything which can collect analytics
type Collector interface {
	Start()
	Gauge(name string, value float64)
	Stop()
}

var std Collector

// Configure configures the default collector
func Configure(username string, token string, source string, timeout time.Duration, waitGroup *sync.WaitGroup) {
	std = NewCollector(username, token, source, timeout, waitGroup)
}

// Start starts the analytics collector
func Start() {
	if std != nil {
		std.Start()
	}
}

// Gauge records a new gauge value
func Gauge(name string, value float64) {
	if std != nil {
		std.Gauge(name, value)
	}
}

// Stop stops the analytics collector
func Stop() {
	if std != nil {
		std.Stop()
	}
}
