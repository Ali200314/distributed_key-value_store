package store

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
)

var _raftTimeout = 5 * time.Minute

const (
	_set    = "set"
	_delete = "delete"

	LeaderRead  int64 = 0
	AnyNodeRead int64 = 1
)

type Store interface {
	Create(string, string) error
	Read(string) (string, int64, error)
	Delete(string) error
	Update(string, string) error
	NodeAddresses() []string
	LeaderAddress() string
	SetRaft(r *raft.Raft)
	SetConsistency(c int64)
	HandleConsistencyInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error)
	SetConsistencyInterceptor(
		ctx context.Context,
		method string,
		req, reply any,
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error
}

type store struct {
	raft        *raft.Raft
	mtx         sync.Mutex
	fsm         *Fsm
	consistency atomic.Int64
}

func (s *store) SetConsistency(c int64) {
	s.consistency.Store(c)
}

func New(f *Fsm) Store {
	return &store{
		fsm: f,
	}
}

func (s *store) Create(key string, val string) error {
	if s.raft.State() != raft.Leader {
		return errors.New("follower unable to create")
	}
	cmd := command{
		Operation: _set,
		Key:       key,
		Value:     val,
	}
	cmdMarshaled, err := json.Marshal(cmd)
	if err != nil {
		return errors.New("unable to marshal command")
	}
	return s.raft.Apply(cmdMarshaled, _raftTimeout).Error()
}

func (s *store) Read(key string) (string, int64, error) {
	c := s.consistency.Load()
	if s.raft.State() != raft.Leader && c != AnyNodeRead {
		return "", c, errors.New("follower unable to serve read")
	}
	s.mtx.Lock()
	defer s.mtx.Unlock()
	return s.fsm.Read(key), c, nil
}

func (s *store) Delete(key string) error {
	if s.raft.State() != raft.Leader {
		return errors.New("follower unable to delete")
	}
	cmd := command{
		Operation: _delete,
		Key:       key,
	}
	cmdMarshaled, err := json.Marshal(cmd)
	if err != nil {
		return errors.New("unable to marshal command")
	}
	return s.raft.Apply(cmdMarshaled, _raftTimeout).Error()
}

func (s *store) Update(key string, val string) error {
	if s.raft.State() != raft.Leader {
		return errors.New("follower unable to update")
	}
	cmd := command{
		Operation: _set,
		Key:       key,
		Value:     val,
	}
	cmdMarshaled, err := json.Marshal(cmd)
	if err != nil {
		return errors.New("unable to marshal command")
	}
	return s.raft.Apply(cmdMarshaled, _raftTimeout).Error()
}

func (s *store) NodeAddresses() []string {
	var addresses []string
	cfg := s.raft.GetConfiguration()
	for _, serv := range cfg.Configuration().Servers {
		addr := []byte(string(serv.Address))
		addr[12] = '7'
		addresses = append(addresses, string(addr))
	}
	return addresses
}
func (s *store) LeaderAddress() string {
	addr, _ := s.raft.LeaderWithID()
	addrStr := []byte(string(addr))
	addrStr[12] = '7'
	return string(addrStr)
}

func (s *store) SetRaft(r *raft.Raft) {
	s.raft = r
}
