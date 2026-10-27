package consistency

import (
	"context"
	"fmt"
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
	clients                 []ConsistencyServiceClient
	mtx                     sync.Mutex
	histogram               *hdr.Histogram
	shutdown                chan bool
	CurReadConsistency      int32
	latencyThreshold        int
	cpuUtilizationThreshold int
}

func New(l, c int, clients []ConsistencyServiceClient) *server {
	return &server{
		histogram:               hdr.New(1, 50_000, 4),
		shutdown:                make(chan bool, 1),
		latencyThreshold:        l,
		cpuUtilizationThreshold: c,
		clients:                 clients,
	}
}

func (s *server) UpdateReadConsistency(ctx context.Context, req *ReadConsistencyRequest) (*ReadConsistencyResponse, error) {
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
		return &ReadConsistencyResponse{}, nil
	}

	return &ReadConsistencyResponse{}, nil
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

func (s *server) RecordLatency(latency int64) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	_ = s.histogram.RecordValue(latency)
}

func (s *server) ReadConsistency() ReadConsistency {
	return ReadConsistency(
		atomic.LoadInt32(&s.CurReadConsistency),
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

	fmt.Printf("cpu usage: %f - ", percent)
	fmt.Printf("p99 latency: %d - ", pvm[99])

	if pvm[99.0] > 100 && percent > 1 {
		for _, client := range s.clients {
			_, err := client.UpdateReadConsistency(ctx, &ReadConsistencyRequest{Consistency: ReadConsistency_ANY_NODE_READ})
			if err != nil {
				fmt.Println("error sending mode to clients")
			}
		}
		atomic.StoreInt32(
			&s.CurReadConsistency,
			int32(AnyNodeRead),
		)
		fmt.Print("any node read")
	} else {
		for _, client := range s.clients {
			client.UpdateReadConsistency(ctx, &ReadConsistencyRequest{Consistency: ReadConsistency_LEADER_READ})
		}
		atomic.StoreInt32(
			&s.CurReadConsistency,
			int32(LeaderRead),
		)
		fmt.Print("leader read")
	}
	fmt.Print("\n")
}
