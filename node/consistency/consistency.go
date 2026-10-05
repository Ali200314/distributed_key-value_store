package consistency

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	hdr "github.com/HdrHistogram/hdrhistogram-go"
)

type ReadConsistency int32

const (
	LeaderRead ReadConsistency = iota
	AnyNodeRead
)

type Consistency interface {
	// called by key-value API
	RecordLatency(int64)

	// called by store
	ReadConsistency() ReadConsistency
	StartConsistencyHeartbeat()
	StopConsistencyHeartbeat()
}

type consistency struct {
	mtx             sync.Mutex
	histogram       *hdr.Histogram
	readConsistency int32

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func (c *consistency) RecordLatency(latency int64) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	_ = c.histogram.RecordValue(latency)
}

func (c *consistency) ReadConsistency() ReadConsistency {
	return ReadConsistency(
		atomic.LoadInt32(&c.readConsistency),
	)
}

func (c *consistency) StartConsistencyHeartbeat() {
	c.mtx.Lock()

	if c.cancel != nil {
		c.mtx.Unlock()
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel

	c.wg.Add(1)
	c.mtx.Unlock()

	go func() {
		defer c.wg.Done()

		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				c.handleHeartbeat()

			case <-ctx.Done():
				return
			}
		}
	}()
}

func (c *consistency) StopConsistencyHeartbeat() {
	c.mtx.Lock()

	if c.cancel == nil {
		c.mtx.Unlock()
		return
	}

	cancel := c.cancel
	c.cancel = nil

	c.mtx.Unlock()

	cancel()
	c.wg.Wait()
}

func (c *consistency) handleHeartbeat() {
	// TODO: calculate leader load.
	load := c.calculateLeaderLoad()

	if load > 0.8 {
		atomic.StoreInt32(
			&c.readConsistency,
			int32(AnyNodeRead),
		)
	} else {
		atomic.StoreInt32(
			&c.readConsistency,
			int32(LeaderRead),
		)
	}
}

func (c *consistency) calculateLeaderLoad() float64 {
	// TODO: calculate actual leader load.
	return 0
}