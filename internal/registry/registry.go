package registry

import (
	"context"
	"reflect"
	"sync"

	"github.com/aredoff/eventbus/internal/list"
)

type Key struct {
	typ reflect.Type
}

type Entry struct {
	Handle   any
	Dispatch func(ctx context.Context, payload any)
}

type Registry struct {
	subs sync.Map
}

func KeyOf[T any]() Key {
	return Key{typ: reflect.TypeFor[*T]()}
}

func Same(a, b any) bool {
	if a == nil || b == nil {
		return a == b
	}

	va := reflect.ValueOf(a)
	vb := reflect.ValueOf(b)
	if va.Type() != vb.Type() {
		return false
	}
	if va.Kind() == reflect.Func {
		return va.Pointer() == vb.Pointer()
	}

	return a == b
}

func (r *Registry) Add(key Key, entry Entry) {
	v, _ := r.subs.LoadOrStore(key, &list.AtomicList[Entry]{})
	al := v.(*list.AtomicList[Entry])
	al.Add(entry)
}

func (r *Registry) Remove(key Key, handle any) bool {
	v, ok := r.subs.Load(key)
	if !ok {
		return false
	}

	al := v.(*list.AtomicList[Entry])
	return al.Remove(func(e Entry) bool {
		return Same(e.Handle, handle)
	})
}

func (r *Registry) Dispatch(key Key, ctx context.Context, payload any) {
	v, ok := r.subs.Load(key)
	if !ok {
		return
	}

	al := v.(*list.AtomicList[Entry])
	al.ForEach(func(entry Entry) bool {
		entry.Dispatch(ctx, payload)
		return true
	})
}

func (r *Registry) Subscribers() int64 {
	var n int64

	r.subs.Range(func(_, val any) bool {
		al := val.(*list.AtomicList[Entry])
		n += al.Size()
		return true
	})

	return n
}

func (r *Registry) SubscribersFor(key Key) int64 {
	v, ok := r.subs.Load(key)
	if !ok {
		return 0
	}
	return v.(*list.AtomicList[Entry]).Size()
}
