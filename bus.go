package eventbus

import (
	"context"
	"sync"

	"github.com/aredoff/eventbus/internal/dispatch"
	"github.com/aredoff/eventbus/internal/registry"
)

type Bus struct {
	cfg       Config
	reg       *registry.Registry
	disp      *dispatch.Dispatcher
	globalMW  []AnyMiddleware
	closeOnce sync.Once
}

func New(cfg Config) *Bus {
	cfg = cfg.withDefaults()

	reg := &registry.Registry{}

	b := &Bus{
		cfg:  cfg,
		reg:  reg,
		disp: dispatch.New(cfg.QueueSize, reg),
	}

	return b
}

func (b *Bus) Use(mws ...AnyMiddleware) {
	b.globalMW = append(b.globalMW, mws...)
}

func (b *Bus) Subscribers() int64 {
	return b.reg.Subscribers()
}

func (b *Bus) Close(ctx context.Context) {
	b.closeOnce.Do(func() {
		b.disp.Close(ctx)
	})
}

func (b *Bus) registry() *registry.Registry {
	return b.reg
}

func (b *Bus) dispatcher() *dispatch.Dispatcher {
	return b.disp
}

func (b *Bus) middleware(opts subOptions) []AnyMiddleware {
	out := make([]AnyMiddleware, 0, len(b.globalMW)+len(opts.middleware))
	out = append(out, b.globalMW...)
	out = append(out, opts.middleware...)
	return out
}
