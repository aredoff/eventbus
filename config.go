package eventbus

import (
	"log/slog"
)

type Config struct {
	QueueSize int
	Logger    *slog.Logger
}

func (c Config) withDefaults() Config {
	if c.QueueSize <= 0 {
		c.QueueSize = 512
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	return c
}
