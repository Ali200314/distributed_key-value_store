package consistency

import (
	"container/ring"
	"sync"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
)

type Window struct {
	mtx    sync.Mutex
	bucket *ring.Ring
}

func NewLatencyWindow() *Window {
	r := ring.New(30)
	for range 30 {
		r.Value = hdrhistogram.New(1, 50_000, 3)
		r = r.Next()
	}
	return &Window{
		bucket: r,
	}

}

func (l *Window) RecordLatency(latencyMs int64) error {
	now := time.Now().UnixMilli()

	l.mtx.Lock()
	defer l.mtx.Unlock()

	bucket := l.bucket.Value.(*hdrhistogram.Histogram)
	if now-bucket.StartTimeMs() < 1000 {
		return bucket.RecordValue(latencyMs)
	}

	l.bucket = l.bucket.Next()
	bucket = l.bucket.Value.(*hdrhistogram.Histogram)
	bucket.Reset()
	bucket.SetStartTimeMs(now)
	return bucket.RecordValue(latencyMs)
}

func (l *Window) PercentileMs(percentile float64) int64 {
	l.mtx.Lock()
	defer l.mtx.Unlock()

	lookback := time.Now().Add(-30 * time.Second).UnixMilli()

	histogram := hdrhistogram.New(1, 50_000, 3)
	for i := 0; i < l.bucket.Len(); i++ {
		bucket := l.bucket.Value.(*hdrhistogram.Histogram)
		if bucket.StartTimeMs() >= lookback {
			histogram.Merge(bucket)
		}
		l.bucket = l.bucket.Next()
	}
	if histogram.TotalCount() == 0 {
		return 0
	}
	return histogram.ValueAtPercentiles([]float64{percentile})[percentile]
}
