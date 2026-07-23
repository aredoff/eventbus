package middleware

import (
	"context"
	"log/slog"
	"reflect"
	"runtime/debug"
	"time"

	"github.com/aredoff/eventbus"
)

func Recover(logger *slog.Logger) eventbus.AnyMiddleware {
	if logger == nil {
		logger = slog.Default()
	}

	return func(next eventbus.AnyHandler) eventbus.AnyHandler {
		return func(ctx context.Context, payload any) {
			defer func() {
				if r := recover(); r != nil {
					logger.Error("handler panic",
						"panic", r,
						"stack", string(debug.Stack()),
					)
				}
			}()

			next(ctx, payload)
		}
	}
}

func Timeout(d time.Duration) eventbus.AnyMiddleware {
	return func(next eventbus.AnyHandler) eventbus.AnyHandler {
		return func(ctx context.Context, payload any) {
			ctx, cancel := context.WithTimeout(ctx, d)
			defer cancel()

			next(ctx, payload)
		}
	}
}

func Logging(logger *slog.Logger) eventbus.AnyMiddleware {
	if logger == nil {
		logger = slog.Default()
	}

	return func(next eventbus.AnyHandler) eventbus.AnyHandler {
		return func(ctx context.Context, payload any) {
			start := time.Now()
			next(ctx, payload)
			t := reflect.TypeOf(payload)
			event := t.String()
			if t.Kind() == reflect.Ptr && t.Elem() != nil {
				event = t.Elem().String()
			}
			logger.Debug("handler completed",
				"event", event,
				"duration", time.Since(start),
			)
		}
	}
}
