package list_test

import (
	"sync"
	"testing"

	"github.com/aredoff/eventbus/internal/list"
	"github.com/stretchr/testify/assert"
)

func TestAtomicList(t *testing.T) {
	var al list.AtomicList[int]

	al.Add(1)
	al.Add(2)
	al.Add(3)

	assert.Equal(t, int64(3), al.Size())

	var values []int
	al.ForEach(func(v int) bool {
		values = append(values, v)
		return true
	})
	assert.Len(t, values, 3)
	assert.Contains(t, values, 1)
	assert.Contains(t, values, 2)
	assert.Contains(t, values, 3)

	assert.True(t, al.Remove(func(v int) bool { return v == 2 }))
	assert.Equal(t, int64(2), al.Size())

	values = nil
	al.ForEach(func(v int) bool {
		values = append(values, v)
		return true
	})
	assert.NotContains(t, values, 2)
}

func TestAtomicListConcurrent(t *testing.T) {
	var al list.AtomicList[int]

	const n = 1000
	var wg sync.WaitGroup
	wg.Add(n)

	for i := range n {
		go func(v int) {
			defer wg.Done()
			al.Add(v)
		}(i)
	}

	wg.Wait()
	assert.Equal(t, int64(n), al.Size())
}
