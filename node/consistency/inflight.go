package consistency

import "sync/atomic"

type InflightCounter struct {
	count atomic.Int64
}

func (r *InflightCounter) Increment() {
	r.count.Add(1)
}

func (r *InflightCounter) Decrement() {
	r.count.Add(-1)
}

func (r *InflightCounter) Count() int64 {
	return r.count.Load()
}
