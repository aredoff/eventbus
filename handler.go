package eventbus

import "context"

type AnyHandler func(ctx context.Context, payload any)

type AnyMiddleware func(AnyHandler) AnyHandler

type Handler[T any] func(ctx context.Context, payload *T)

type Middleware[T any] func(Handler[T]) Handler[T]

func Chain[T any](h Handler[T], mws ...Middleware[T]) Handler[T] {
	for i := len(mws) - 1; i >= 0; i-- {
		h = mws[i](h)
	}
	return h
}

type Option func(*subOptions)

type subOptions struct {
	middleware []AnyMiddleware
}

func WithMiddleware(mws ...AnyMiddleware) Option {
	return func(o *subOptions) {
		o.middleware = append(o.middleware, mws...)
	}
}

func adaptMiddleware[T any](mw AnyMiddleware) Middleware[T] {
	return func(next Handler[T]) Handler[T] {
		anyNext := AnyHandler(func(ctx context.Context, payload any) {
			next(ctx, payload.(*T))
		})
		wrapped := mw(anyNext)
		return func(ctx context.Context, payload *T) {
			wrapped(ctx, payload)
		}
	}
}

func globalMiddlewareFor[T any](mws []AnyMiddleware) []Middleware[T] {
	out := make([]Middleware[T], len(mws))
	for i, mw := range mws {
		out[i] = adaptMiddleware[T](mw)
	}
	return out
}
