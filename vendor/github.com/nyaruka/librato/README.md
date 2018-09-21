# Librato [![Build Status](https://travis-ci.org/nyaruka/librato.svg?branch=master)](https://travis-ci.org/nyaruka/librato) [![Coverage Status](https://coveralls.io/repos/github/nyaruka/librato/badge.svg?branch=master)](https://coveralls.io/github/nyaruka/librato?branch=master) [![Go Report Card](https://goreportcard.com/badge/github.com/nyaruka/librato)](https://goreportcard.com/report/github.com/nyaruka/librato)

Basic Librato client library with batching of events. Thus far it only supports sending gauge values
because that's all we need, but contributions welcome.

## Usage

You can either instantiate a collector and use that:

```go
import "github.com/nyaruka/librato"

collector := librato.NewCollector(...)
collector.Start()
collector.Gauge("awesomeness.level", 10)
collector.Gauge("foo.count", 123.45)
collector.Stop()
```

Or configure the default collector and use it like:

```go
librato.Configure(...)
librato.Start()
librato.Gauge("awesomeness.level", 10)
librato.Gauge("foo.count", 123.45)
librato.Stop()
```
