package eventbus

import (
	disp "github.com/aredoff/eventbus/internal/dispatch"
)

var (
	ErrQueueFull = disp.ErrQueueFull
	ErrClosed    = disp.ErrClosed
)
