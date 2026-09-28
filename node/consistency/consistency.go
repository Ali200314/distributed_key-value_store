package consistency

import (
	"sync"

	hdr "github.com/HdrHistogram/hdrhistogram-go"
)

type ReadConsistency int

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
	readConsistency ReadConsistency
}

func (c *consistency) RecordLatency(latency int64) {
	defer c.mtx.Unlock()
	c.mtx.Lock()
	if err := c.histogram.RecordValue(latency); err != nil {
	}
}

func (s *consistency) ReadConsistency() ReadConsistency {
}

func (s *consistency) StartConsistencyHeartbeat() {
	// loop forever
	// 		calculate leader load
	// 		if load is high then tell followers nodes to use anyNodeRead
	// 		else tell followers to use leaderRead
}

func (s *consistency) StopConsistencyHeartbeat() {
	// stop forever loop
}

func (s *consistency) handleHeartbeat() {
	// set internal read consistency
}
