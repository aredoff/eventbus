// Package eventbus provides a type-safe in-process event bus for Go monoliths.
//
// Events are routed by Go type. Multiple subscribers can listen to the same type.
// Each subscribed type gets its own bounded queue — backpressure on one type
// does not block others.
//
// Publishing:
//   - Pub — async, fire-and-forget; returns ErrQueueFull when the type queue is full
//   - PubSync — synchronous, bypasses the queue; use for critical paths
//
// Heavy or slow handlers: forward to a channel and process in a dedicated worker pool.
package eventbus
