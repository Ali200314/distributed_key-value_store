package consistency

import (
	"context"
	"distributedkvstore/api"
	"distributedkvstore/store"
	"fmt"
	"sync"
	"time"

	"distributedkvstore/latency"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type server struct {
	mtx               sync.Mutex
	latencyWindow     *latency.Window
	shutdown          chan bool
	readConsistency   int64
	latencyThreshold  int64
	inflightTracker   *api.InflightTracker
	inflightThreshold int64
	nodeID            int
	store             store.Store
}

func New(
	latencyWindow *latency.Window,
	latencyThreshold, inflightThreshold int64,
	inflightTracker *api.InflightTracker,
	nodeID int,
	store store.Store,
) *server {
	return &server{
		latencyWindow:     latencyWindow,
		shutdown:          make(chan bool, 1),
		latencyThreshold:  latencyThreshold,
		inflightTracker:   inflightTracker,
		inflightThreshold: inflightThreshold,
		nodeID:            nodeID,
		store:             store,
	}
}

type Consistency interface {
	RecordLatency(int64)
	StartHeartbeat(context.Context)
	StopHeartbeat()
}

func (s *server) RecordLatency(latency int64) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.latencyWindow.RecordLatency(latency)
}

func (s *server) StartHeartbeat(ctx context.Context) {
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

func (s *server) StopHeartbeat() {
	s.shutdown <- true
}

var (
	leader = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "raft_leader_id",
		Help: "current leader",
	})
	consistency = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "consistency",
	})
)

func (s *server) HandleHeartBeat(ctx context.Context) {
	leader.Set(float64(s.nodeID))

	p99 := s.latencyWindow.PercentileMs(99.0)

	count := s.inflightTracker.Count()
	fmt.Printf("inflight req count: %d - ", count)
	fmt.Printf("p99 latency: %d - ", p99)

	if p99 > s.latencyThreshold && count > s.inflightThreshold {
		s.store.SetConsistency(store.AnyNodeRead)
		fmt.Print("any node read")
	} else {
		s.store.SetConsistency(store.LeaderRead)
		fmt.Print("leader read")
	}
	fmt.Print("\n")
}
