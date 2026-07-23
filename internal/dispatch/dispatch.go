package dispatch

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/aredoff/eventbus/internal/registry"
)

type Envelope struct {
	Ctx     context.Context
	Key     registry.Key
	Payload any
}

type typeQueue struct {
	ch   chan *Envelope
	once sync.Once
}

type Dispatcher struct {
	queues    sync.Map
	queueSize int
	pool      sync.Pool
	closed    atomic.Bool
	reg       *registry.Registry
	closeWG   sync.WaitGroup
}

func New(queueSize int, reg *registry.Registry) *Dispatcher {
	d := &Dispatcher{
		queueSize: queueSize,
		reg:       reg,
	}
	d.pool.New = func() any {
		return &Envelope{}
	}
	return d
}

func (d *Dispatcher) EnsureQueue(key registry.Key) {
	if d.closed.Load() {
		return
	}

	v, _ := d.queues.LoadOrStore(key, &typeQueue{})
	tq := v.(*typeQueue)
	tq.once.Do(func() {
		tq.ch = make(chan *Envelope, d.queueSize)
		d.closeWG.Add(1)
		go d.consume(key, tq)
	})
}

func (d *Dispatcher) consume(key registry.Key, tq *typeQueue) {
	defer d.closeWG.Done()

	for env := range tq.ch {
		d.reg.Dispatch(key, env.Ctx, env.Payload)
		d.pool.Put(env)
	}
}

func (d *Dispatcher) Enqueue(ctx context.Context, key registry.Key, payload any) error {
	if d.closed.Load() {
		return ErrClosed
	}

	v, ok := d.queues.Load(key)
	if !ok {
		return nil
	}

	tq := v.(*typeQueue)
	if tq.ch == nil {
		return nil
	}

	env := d.pool.Get().(*Envelope)
	env.Ctx = ctx
	env.Key = key
	env.Payload = payload

	select {
	case tq.ch <- env:
		return nil
	default:
		d.pool.Put(env)
		return ErrQueueFull
	}
}

func (d *Dispatcher) DispatchSync(ctx context.Context, key registry.Key, payload any) {
	d.reg.Dispatch(key, ctx, payload)
}

func (d *Dispatcher) Close(ctx context.Context) {
	if d.closed.Swap(true) {
		return
	}

	done := make(chan struct{})
	go func() {
		d.queues.Range(func(_, val any) bool {
			tq := val.(*typeQueue)
			if tq.ch != nil {
				close(tq.ch)
			}
			return true
		})
		d.closeWG.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
	}
}

func (d *Dispatcher) HasQueue(key registry.Key) bool {
	v, ok := d.queues.Load(key)
	if !ok {
		return false
	}
	return v.(*typeQueue).ch != nil
}
