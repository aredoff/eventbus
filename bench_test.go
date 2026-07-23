package eventbus_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/aredoff/eventbus"
)

func BenchmarkPub(b *testing.B) {
	subs := []int{1, 8, 64}

	for _, n := range subs {
		b.Run(fmt.Sprintf("%02d_subscribers", n), func(b *testing.B) {
			bus := eventbus.New(eventbus.Config{QueueSize: 65536})
			defer bus.Close(context.Background())

			for range n {
				eventbus.Sub(bus, func(_ context.Context, _ *order) {
				})
			}

			ctx := context.Background()
			o := order{ID: 1}

			b.ResetTimer()

			for range b.N {
				_ = eventbus.Pub(bus, ctx, &o)
			}
		})
	}
}

func BenchmarkPubParallel(b *testing.B) {
	subs := []int{1, 8, 64}

	for _, n := range subs {
		b.Run(fmt.Sprintf("%02d_subscribers", n), func(b *testing.B) {
			bus := eventbus.New(eventbus.Config{QueueSize: 65536})
			defer bus.Close(context.Background())

			for range n {
				eventbus.Sub(bus, func(_ context.Context, _ *order) {
				})
			}

			ctx := context.Background()

			b.ResetTimer()

			b.RunParallel(func(p *testing.PB) {
				var o order
				for p.Next() {
					_ = eventbus.Pub(bus, ctx, &o)
				}
			})
		})
	}
}

func BenchmarkPubSync(b *testing.B) {
	bus := eventbus.New(eventbus.Config{QueueSize: 8})
	defer bus.Close(context.Background())

	eventbus.Sub(bus, func(_ context.Context, _ *order) {
	})

	ctx := context.Background()
	o := order{ID: 1}

	b.ResetTimer()

	for range b.N {
		eventbus.PubSync(bus, ctx, &o)
	}
}
