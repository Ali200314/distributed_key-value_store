package consistency

import (
	"context"
	"distributedkvstore/store"
	"fmt"
	"time"
)

type consistency struct {
	latencyWindow      *Window
	shutdown           chan bool
	latencyUpperTH     int64
	inflightUpperTH    int64
	nodeID             int
	inflightCounter    *InflightCounter
	stabilizationCount int
	latencyLowerTH     int64
	inflightLowerTH    int64
	store              store.Store
}

func New(
	latencyWindow *Window,
	latencyThreshold, inflightThreshold int64,
	recoveryLatencyThreshold, recoveryInflightThreshold int64,
	inflightCounter *InflightCounter,
	store store.Store,
	nodeID int,
) Consistency {
	return &consistency{
		latencyWindow:   latencyWindow,
		shutdown:        make(chan bool, 1),
		latencyUpperTH:  latencyThreshold,
		inflightCounter: inflightCounter,
		inflightUpperTH: inflightThreshold,
		nodeID:          nodeID,
		latencyLowerTH:  recoveryLatencyThreshold,
		inflightLowerTH: recoveryInflightThreshold,
	}
}

type Consistency interface {
	StartHeartbeat(context.Context)
	StopHeartbeat()
}

func (s *consistency) StartHeartbeat(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				s.HandleHeartBeat(ctx)
			case <-s.shutdown:
				return
			}
		}
	}()
}

func (s *consistency) StopHeartbeat() {
	s.shutdown <- true
}

func (s *consistency) HandleHeartBeat(ctx context.Context) {

	p99 := s.latencyWindow.PercentileMs(99.0)
	count := s.inflightCounter.Count()

	fmt.Printf("inflight req count: %d - ", count)
	fmt.Printf("p99 latency: %d - ", p99)

	if p99 > s.latencyUpperTH && count > s.inflightUpperTH {
		s.stabilizationCount = 0
		s.store.SetConsistency(store.AnyNodeRead)
		return
	}

	if p99 < s.latencyLowerTH && count < s.inflightLowerTH {
		s.stabilizationCount++
		if s.stabilizationCount >= 8 { // e.g., 8
			s.stabilizationCount = 0
			s.store.SetConsistency(store.LeaderRead)
		}
	} else {
		s.stabilizationCount = 0
	}
}
