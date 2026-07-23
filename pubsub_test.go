package eventbus_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aredoff/eventbus"
	"github.com/aredoff/eventbus/middleware"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

type order struct {
	ID int
}

type user struct {
	Name string
}

type started struct{}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestSubPubSync(t *testing.T) {
	bus := eventbus.New(eventbus.Config{QueueSize: 8})
	defer bus.Close(context.Background())

	var got atomic.Int32

	handler := func(_ context.Context, o *order) {
		got.Add(int32(o.ID))
	}

	eventbus.Sub(bus, handler)
	eventbus.PubSync(bus, context.Background(), &order{ID: 10})

	assert.Equal(t, int32(10), got.Load())
	assert.Equal(t, int64(1), bus.Subscribers())
}

func TestSubPubAsync(t *testing.T) {
	bus := eventbus.New(eventbus.Config{QueueSize: 64})
	defer bus.Close(context.Background())

	done := make(chan struct{}, 1)

	eventbus.Sub(bus, func(_ context.Context, o *order) {
		done <- struct{}{}
		assert.Equal(t, 42, o.ID)
	})

	require.NoError(t, eventbus.Pub(bus, context.Background(), &order{ID: 42}))

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for async handler")
	}
}

func TestUnsub(t *testing.T) {
	bus := eventbus.New(eventbus.Config{QueueSize: 8})
	defer bus.Close(context.Background())

	var calls atomic.Int32

	handler := func(_ context.Context, _ *order) {
		calls.Add(1)
	}

	eventbus.Sub(bus, handler)
	eventbus.PubSync(bus, context.Background(), &order{ID: 1})

	require.True(t, eventbus.Unsub(bus, handler))
	eventbus.PubSync(bus, context.Background(), &order{ID: 2})

	assert.Equal(t, int32(1), calls.Load())
}

func TestTypedRouting(t *testing.T) {
	bus := eventbus.New(eventbus.Config{QueueSize: 8})
	defer bus.Close(context.Background())

	var users, orders atomic.Int32

	eventbus.Sub(bus, func(_ context.Context, _ *user) {
		users.Add(1)
	})

	eventbus.Sub(bus, func(_ context.Context, _ *order) {
		orders.Add(1)
	})

	eventbus.PubSync(bus, context.Background(), &user{Name: "a"})
	eventbus.PubSync(bus, context.Background(), &order{ID: 1})

	assert.Equal(t, int32(1), users.Load())
	assert.Equal(t, int32(1), orders.Load())
}

func TestMultipleSubscribersSameType(t *testing.T) {
	bus := eventbus.New(eventbus.Config{QueueSize: 8})
	defer bus.Close(context.Background())

	var a, b atomic.Int32

	eventbus.Sub(bus, func(_ context.Context, _ *order) {
		a.Add(1)
	})
	eventbus.Sub(bus, func(_ context.Context, _ *order) {
		b.Add(1)
	})

	eventbus.PubSync(bus, context.Background(), &order{ID: 1})

	assert.Equal(t, int32(1), a.Load())
	assert.Equal(t, int32(1), b.Load())
	assert.Equal(t, int64(2), bus.Subscribers())
}

func TestNoSubscribers(t *testing.T) {
	bus := eventbus.New(eventbus.Config{QueueSize: 8})
	defer bus.Close(context.Background())

	require.NoError(t, eventbus.Pub(bus, context.Background(), &order{ID: 1}))
	eventbus.PubSync(bus, context.Background(), &order{ID: 2})
}

func TestQueueIsolation(t *testing.T) {
	bus := eventbus.New(eventbus.Config{QueueSize: 1})
	defer bus.Close(context.Background())

	block := make(chan struct{})
	eventbus.Sub(bus, func(_ context.Context, _ *order) {
		<-block
	})

	require.NoError(t, eventbus.Pub(bus, context.Background(), &order{ID: 1}))

	userDone := make(chan struct{})
	eventbus.Sub(bus, func(_ context.Context, _ *user) {
		close(userDone)
	})
	require.NoError(t, eventbus.Pub(bus, context.Background(), &user{Name: "x"}))

	select {
	case <-userDone:
	case <-time.After(time.Second):
		t.Fatal("user event blocked by order handler")
	}

	close(block)
}

func TestGracefulClose(t *testing.T) {
	bus := eventbus.New(eventbus.Config{QueueSize: 64})

	const n = 50
	var count atomic.Int32

	eventbus.Sub(bus, func(_ context.Context, _ *order) {
		count.Add(1)
	})

	for i := range n {
		require.NoError(t, eventbus.Pub(bus, context.Background(), &order{ID: i}))
	}

	bus.Close(context.Background())
	assert.Equal(t, int32(n), count.Load())
}

func TestPubWithoutSubscribers(t *testing.T) {
	bus := eventbus.New(eventbus.Config{QueueSize: 8})
	defer bus.Close(context.Background())

	require.NoError(t, eventbus.Pub(bus, context.Background(), &order{ID: 1}))
}

func TestQueueFull(t *testing.T) {
	bus := eventbus.New(eventbus.Config{QueueSize: 1})
	defer bus.Close(context.Background())

	block := make(chan struct{})
	eventbus.Sub(bus, func(_ context.Context, _ *order) {
		<-block
	})

	require.NoError(t, eventbus.Pub(bus, context.Background(), &order{ID: 1}))

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if err := eventbus.Pub(bus, context.Background(), &order{ID: 2}); errors.Is(err, eventbus.ErrQueueFull) {
			close(block)
			return
		}
		time.Sleep(time.Millisecond)
	}

	close(block)
	t.Fatal("expected ErrQueueFull")
}

func TestRecoverMiddleware(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	bus := eventbus.New(eventbus.Config{QueueSize: 8, Logger: logger})
	defer bus.Close(context.Background())

	bus.Use(middleware.Recover(logger))

	var called atomic.Bool
	eventbus.Sub(bus, func(_ context.Context, _ *order) {
		panic("boom")
	})
	eventbus.Sub(bus, func(_ context.Context, _ *order) {
		called.Store(true)
	})

	eventbus.PubSync(bus, context.Background(), &order{ID: 1})
	assert.True(t, called.Load())
}

func TestSignalType(t *testing.T) {
	bus := eventbus.New(eventbus.Config{QueueSize: 8})
	defer bus.Close(context.Background())

	var n atomic.Int32
	eventbus.Sub(bus, func(_ context.Context, _ *started) {
		n.Add(1)
	})

	eventbus.PubSync(bus, context.Background(), &started{})

	assert.Equal(t, int32(1), n.Load())
}

func TestParallelPublish(t *testing.T) {
	bus := eventbus.New(eventbus.Config{QueueSize: 8192})
	defer bus.Close(context.Background())

	const publishers = 32
	const perPublisher = 100
	const totalEvents = publishers * perPublisher

	var total atomic.Int64
	var handled sync.WaitGroup
	handled.Add(totalEvents)

	eventbus.Sub(bus, func(_ context.Context, o *order) {
		total.Add(int64(o.ID))
		handled.Done()
	})

	var wg sync.WaitGroup
	wg.Add(publishers)

	for p := range publishers {
		go func(base int) {
			defer wg.Done()
			for i := range perPublisher {
				id := base*perPublisher + i + 1
				for {
					err := eventbus.Pub(bus, context.Background(), &order{ID: id})
					if err == nil {
						break
					}
					if !errors.Is(err, eventbus.ErrQueueFull) {
						t.Error(err)
						return
					}
					time.Sleep(time.Millisecond)
				}
			}
		}(p)
	}

	wg.Wait()

	done := make(chan struct{})
	go func() {
		handled.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for handlers")
	}

	want := int64(0)
	for p := range publishers {
		for i := range perPublisher {
			want += int64(p*perPublisher + i + 1)
		}
	}
	assert.Equal(t, want, total.Load())
}

func TestClose(t *testing.T) {
	bus := eventbus.New(eventbus.Config{QueueSize: 8})

	bus.Close(context.Background())
	assert.ErrorIs(t, eventbus.Pub(bus, context.Background(), &order{}), eventbus.ErrClosed)
}

func TestSubViaChan(t *testing.T) {
	bus := eventbus.New(eventbus.Config{QueueSize: 8})
	defer bus.Close(context.Background())

	ch := make(chan order, 1)
	eventbus.Sub(bus, func(_ context.Context, o *order) {
		ch <- *o
	})

	eventbus.PubSync(bus, context.Background(), &order{ID: 7})

	select {
	case o := <-ch:
		assert.Equal(t, 7, o.ID)
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}

func TestPayloadCopied(t *testing.T) {
	bus := eventbus.New(eventbus.Config{QueueSize: 8})
	defer bus.Close(context.Background())

	done := make(chan struct{})
	o := order{ID: 1}

	eventbus.Sub(bus, func(_ context.Context, got *order) {
		assert.Equal(t, 1, got.ID)
		close(done)
	})

	require.NoError(t, eventbus.Pub(bus, context.Background(), &o))
	o.ID = 999

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}
