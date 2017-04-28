# Time collated Librato client

[![GoDoc](https://godoc.org/github.com/dcelasun/librato?status.svg)](https://godoc.org/github.com/dcelasun/librato)

This is a simple, lightweight Librato client that implements `time.Duration` based collation.
In other words, the client makes only 1 request to Librato per `time.Duration`.

Internally, it uses a dynamically resizing channel implementation to support infinite<sup>1</sup> buffers.

# Usage

```go
// Limit requests to once every 5 seconds
duration := time.Duration(5*time.Second)
client := librato.NewTimeCollatedClient("user", "token", "source", duration)

// You can use simple numeric values...
client.GetGauge("my-gauge").Push(1)
// ... or use custom properties
client.GetCounter("my-counter").Push(map[string]interface{}{
    "value": 1.5,
    "source": "example.com",
})

// Close the library, and wait for all requests to finish.
client.Close()
client.Wait()
```

# Contributing

Pull requests are welcome. Please open an issue before making big changes.

<sup>1</sup> Within system memory limits.