package middleware_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aredoff/eventbus"
	"github.com/aredoff/eventbus/middleware"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTimeout(t *testing.T) {
	bus := eventbus.New(eventbus.Config{QueueSize: 8})
	defer bus.Close(context.Background())

	bus.Use(middleware.Timeout(10 * time.Millisecond))

	var called atomic.Bool
	eventbus.Sub(bus, func(ctx context.Context, _ *int) {
		select {
		case <-ctx.Done():
			called.Store(true)
		case <-time.After(50 * time.Millisecond):
		}
	})

	eventbus.PubSync(bus, context.Background(), new(int))
	require.True(t, called.Load())
}

func TestMiddlewareOrder(t *testing.T) {
	bus := eventbus.New(eventbus.Config{QueueSize: 8})
	defer bus.Close(context.Background())

	var order []string

	bus.Use(func(next eventbus.AnyHandler) eventbus.AnyHandler {
		return func(ctx context.Context, payload any) {
			order = append(order, "global")
			next(ctx, payload)
		}
	})

	eventbus.Sub(bus, func(_ context.Context, _ *int) {
		order = append(order, "handler")
	}, eventbus.WithMiddleware(func(next eventbus.AnyHandler) eventbus.AnyHandler {
		return func(ctx context.Context, payload any) {
			order = append(order, "local")
			next(ctx, payload)
		}
	}))

	eventbus.PubSync(bus, context.Background(), new(int))
	assert.Equal(t, []string{"global", "local", "handler"}, order)
}
