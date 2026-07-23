package list

import (
	"sync/atomic"
)

type AtomicList[T any] struct {
	size atomic.Int64
	link atomic.Pointer[link[T]]
}

type link[T any] struct {
	next atomic.Pointer[link[T]]
	val  T
}

func (ac *AtomicList[T]) Add(v T) {
	newLink := &link[T]{val: v}

	for {
		head := ac.link.Load()
		newLink.next.Store(head)

		if ac.link.CompareAndSwap(head, newLink) {
			break
		}
	}

	ac.size.Add(1)
}

func (ac *AtomicList[T]) Remove(fn func(T) bool) bool {
	var prev *link[T]
	curr := ac.link.Load()

	for curr != nil {
		if fn(curr.val) {
			next := curr.next.Load()

			if prev == nil {
				if ac.link.CompareAndSwap(curr, next) {
					ac.size.Add(-1)
					return true
				}
			} else {
				if prev.next.CompareAndSwap(curr, next) {
					ac.size.Add(-1)
					return true
				}
			}
			return false
		}

		prev = curr
		curr = curr.next.Load()
	}

	return false
}

func (ac *AtomicList[T]) ForEach(fn func(T) bool) {
	l := ac.link.Load()

	for l != nil {
		if !fn(l.val) {
			return
		}

		l = l.next.Load()
	}
}

func (ac *AtomicList[T]) Size() int64 {
	return ac.size.Load()
}
