package consistency

import (
	"context"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	hdr "github.com/HdrHistogram/hdrhistogram-go"
	"github.com/shirou/gopsutil/process"
)

type server struct {
	UnimplementedConsistencyServiceServer
	clients            []consistencyServiceClient
	mtx                sync.Mutex
	histogram          *hdr.Histogram
	readConsistency    int32
	shutdown           chan bool
	CurReadConsistency int32
}

func (s *server) UpdateConsistency(ctx context.Context, req *ReadConsistencyRequest) *ReadConsistencyResponse {
	switch req.Consistency {
	case ReadConsistency_LEADER_READ:
		atomic.StoreInt32(
			&s.CurReadConsistency,
			int32(LeaderRead),
		)

	case ReadConsistency_ANY_NODE_READ:
		atomic.StoreInt32(
			&s.CurReadConsistency,
			int32(AnyNodeRead),
		)

	default:
		return &ReadConsistencyResponse{}
	}

	return &ReadConsistencyResponse{}
}

const (
	LeaderRead ReadConsistency = iota
	AnyNodeRead
)

type Consistency interface {
	RecordLatency(int64)
	ReadConsistency() ReadConsistency
	StartConsistencyHeartbeat(context.Context)
	StopConsistencyHeartbeat()
}

func New() Consistency {
	return &server{}
}

func (s *server) RecordLatency(latency int64) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	_ = s.histogram.RecordValue(latency)
}

func (s *server) ReadConsistency() ReadConsistency {
	return ReadConsistency(
		atomic.LoadInt32(&s.readConsistency),
	)
}

func (s *server) StartConsistencyHeartbeat(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(time.Second)
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

func (s *server) StopConsistencyHeartbeat() {
	s.shutdown <- true
}

func (s *server) HandleHeartBeat(ctx context.Context) {
	// calculate leaderload
	pvm := s.histogram.ValueAtPercentiles([]float64{99.0})

	p, _ := process.NewProcess(int32(os.Getpid()))
	percent, err := p.CPUPercent()
	if err != nil {
		log.Fatal(err)
	}

	if pvm[99.0] > 100 && percent > 100 {
		for _, client := range s.clients {
			client.UpdateReadConsistency(ctx, &ReadConsistencyRequest{Consistency: ReadConsistency_ANY_NODE_READ})
		}
		atomic.StoreInt32(
			&s.CurReadConsistency,
			int32(AnyNodeRead),
		)
	} else {
		for _, client := range s.clients {
			client.UpdateReadConsistency(ctx, &ReadConsistencyRequest{Consistency: ReadConsistency_LEADER_READ})
		}
		atomic.StoreInt32(
			&s.CurReadConsistency,
			int32(LeaderRead),
		)
	}
}
