package eventbus

import (
	"context"

	"github.com/aredoff/eventbus/internal/registry"
)

func Pub[T any](bus *Bus, ctx context.Context, val *T) error {
	copied := *val
	key := registry.KeyOf[T]()
	return bus.dispatcher().Enqueue(ctx, key, &copied)
}

func PubSync[T any](bus *Bus, ctx context.Context, val *T) {
	copied := *val
	key := registry.KeyOf[T]()
	bus.dispatcher().DispatchSync(ctx, key, &copied)
}

func Sub[T any](bus *Bus, fn Handler[T], opts ...Option) Handler[T] {
	var o subOptions
	for _, opt := range opts {
		opt(&o)
	}

	mws := globalMiddlewareFor[T](bus.middleware(o))
	wrapped := Chain(fn, mws...)

	key := registry.KeyOf[T]()

	bus.registry().Add(key, registry.Entry{
		Handle: fn,
		Dispatch: func(ctx context.Context, payload any) {
			wrapped(ctx, payload.(*T))
		},
	})

	bus.dispatcher().EnsureQueue(key)

	return fn
}

func Unsub[T any](bus *Bus, fn Handler[T]) bool {
	return bus.registry().Remove(registry.KeyOf[T](), fn)
}
