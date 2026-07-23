# eventbus

Type-safe in-process event bus for Go monoliths. Events are routed by Go type — no string topics, no reflection at dispatch.

```bash
go get github.com/aredoff/eventbus
```

## Quick start

```go
package main

import (
	"context"
	"log/slog"

	"github.com/aredoff/eventbus"
)

type OrderCreated struct {
	ID     int
	Amount float64
}

func main() {
	bus := eventbus.New(eventbus.Config{QueueSize: 512})
	defer bus.Close(context.Background())

	eventbus.Sub(bus, func(_ context.Context, o *OrderCreated) {
		slog.Info("persist order", "id", o.ID)
	})

	eventbus.Sub(bus, func(_ context.Context, o *OrderCreated) {
		slog.Info("charge customer", "id", o.ID, "amount", o.Amount)
	})

	_ = eventbus.Pub(bus, context.Background(), &OrderCreated{ID: 1, Amount: 49.99})
}
```

Both handlers receive the same event. `Pub` returns immediately; handlers run on a background goroutine per event type.

## Pub vs PubSync

| | `Pub` | `PubSync` |
|---|---|---|
| Delivery | async, via queue | inline, on caller goroutine |
| Backpressure | returns `ErrQueueFull` when queue is full | blocks until handlers finish |
| When to use | notifications, analytics, anything non-critical | must complete before continuing |

Retry on full queue:

```go
for {
	err := eventbus.Pub(bus, ctx, event)
	if err == nil {
		break
	}
	if !errors.Is(err, eventbus.ErrQueueFull) {
		return err
	}
	time.Sleep(time.Millisecond)
}
```

## Dispatch model

Each subscribed event type gets its own bounded queue and one consumer goroutine. Handlers for the same type run one after another on that goroutine. A slow handler blocks other handlers of that type, but not other types.

The `context.Context` passed to `Pub` is forwarded to every handler unchanged — put trace IDs or deadlines there yourself.

```
Pub(OrderCreated) ──► queue[OrderCreated] ──► handler billing
                                         └──► handler orders
Pub(PaymentDone)  ──► queue[PaymentDone]  ──► ...   (independent)
```

## Middleware

Global middleware applies to every handler:

```go
bus.Use(
	middleware.Recover(logger),
	middleware.Timeout(5*time.Second),
)
```

Per-handler middleware via `eventbus.WithMiddleware(...)`.

Available in `middleware/`: `Recover`, `Timeout`, `Logging`.

## Slow handlers

Keep handlers short. For I/O-heavy work, hand off to your own worker:

```go
jobs := make(chan OrderCreated, 64)

eventbus.Sub(bus, func(_ context.Context, o *OrderCreated) {
	select {
	case jobs <- *o:
	default:
		slog.Warn("worker backlog full", "order_id", o.ID)
	}
})

go func() {
	for o := range jobs {
		sendEmail(o) // slow, runs outside the bus
	}
}()
```

See [`examples/monolith/main.go`](examples/monolith/main.go).

## Config

```go
bus := eventbus.New(eventbus.Config{
	QueueSize: 512, // per event type, default 512
})
```

Sentinel errors: `eventbus.ErrQueueFull`, `eventbus.ErrClosed`.

`Unsub` removes a handler by function identity — keep the handler in a variable if you plan to unsubscribe later.

## Examples

```bash
go run ./examples/monolith          # demo, stops after 12s
go run ./examples/stress/main.go    # load test
go test ./...
```

## Caveats

- `Pub` copies the payload before enqueueing. Safe for async delivery, costs one allocation per publish.
- Do not call `bus.Close()` from inside a handler — the consumer goroutine deadlocks waiting on itself.
- Handlers for the same type are called in reverse subscription order (last `Sub` runs first).

## License

MIT — see [LICENSE](LICENSE).
