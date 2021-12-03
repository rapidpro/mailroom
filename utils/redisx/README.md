# redisx

redisx is a library of Go utilities built on the [redigo](github.com/gomodule/redigo) redis client library.

## Marker

Creating very large numbers of Redis keys can hurt performance, but putting them all in a single set requires that they all have the same expiration. Marker is a way to have multiple sets based on time intervals, accessible like a single set. You trade accuracy of expiry times for a significantly reduced key space. For example using 2 intervals of 24 hours:

```go
marker := NewMarker("foos", time.Hour*24)
marker.Add(rc, "A")  // time is 2021-12-02T09:00
...
marker.Add(rc, "B")  // time is 2021-12-03T10:00
marker.Add(rc, "C")  // time is 2021-12-03T11:00
```

Creates 2 Redis sets like:

```
foos:2021-12-02 => {"A"}       // expires at 2021-12-04T09:00
foos:2021-12-03 => {"B", "C"}  // expires at 2021-12-05T11:00
```

But can be accessed like a single set:

```go
marker.Contains(rc, "A")   // true
marker.Contains(rc, "B")   // true
marker.Contains(rc, "D")   // false
```

## Cache

Same idea as Marker but for hashes. For example using 2 intervals of 1 hour:

```go
cache := NewCache("foos", time.Hour)
cache.Set(rc, "A", "1")  // time is 2021-12-02T09:10
...
cache.Set(rc, "B", "2")  // time is 2021-12-02T10:15
cache.Set(rc, "C", "3")  // time is 2021-12-02T10:20
```

Creates 2 Redis hashes like:

```
foos:2021-12-02T09:00 => {"A": "1"}            // expires at 2021-12-02T11:10
foos:2021-12-02T10:00 => {"B": "2", "C": "3"}  // expires at 2021-12-02T12:20
```

But can be accessed like a single hash:

```go
cache.Get(rc, "A")   // "1"
cache.Get(rc, "B")   // "2"
cache.Get(rc, "D")   // ""
```

## Series

When getting a value from Cache you're getting the newest value by looking back through the intervals. Series however lets you get an accumulated value from each interval.

For example using 3 intervals of 1 hour:

```go
series := NewSeries("foos", time.Hour, 3)
series.Record(rc, "A", 1)  // time is 2021-12-02T09:10
series.Record(rc, "A", 2)  // time is 2021-12-02T09:15
...
series.Record(rc, "A", 3)  // time is 2021-12-02T10:15
series.Record(rc, "A", 4)  // time is 2021-12-02T10:20
...
series.Record(rc, "A", 5)  // time is 2021-12-02T11:25
series.Record(rc, "B", 1)  // time is 2021-12-02T11:30
```

Creates 3 Redis hashes like:

```
foos:2021-12-02T09:00 => {"A": "3"}            // expires at 2021-12-02T12:15
foos:2021-12-02T10:00 => {"A": "7"}            // expires at 2021-12-02T13:20
foos:2021-12-02T11:00 => {"A": "5", "B": "1"}  // expires at 2021-12-02T14:30
```

But lets us retrieve values across intervals:

```go
series.Get(rc, "A")   // [5, 7, 3]
series.Get(rc, "B")   // [1, 0, 0]
series.Get(rc, "C")   // [0, 0, 0]
```

## Testing Asserts

The `assertredis` package contains several asserts useful for testing the state of a Redis database.