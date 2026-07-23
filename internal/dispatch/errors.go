package dispatch

import "errors"

var (
	ErrQueueFull = errors.New("eventbus: queue full")
	ErrClosed    = errors.New("eventbus: bus closed")
)
