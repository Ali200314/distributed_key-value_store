package store

import (
	"distributedkvstore/consistency"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

var _raftTimeout = 5 * time.Minute

const (
	_set    = "set"
	_delete = "delete"
)

type Store interface {
	Create(string, string) error
	Read(string) (string, error)
	Delete(string) error
	Update(string, string) error
}

type store struct {
	raft        *raft.Raft
	consistency consistency.Consistency
	values      map[string]string
	mtx         sync.Mutex
	fsm         *Fsm
}

func New(r *raft.Raft, c consistency.Consistency, f *Fsm) Store {
	return &store{
		raft:        r,
		consistency: c,
		fsm:         f,
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

func (s *store) Read(key string) (string, error) {
	if s.raft.State() != raft.Leader && s.consistency.ReadConsistency() != consistency.AnyNodeRead {
		return "", errors.New("follower unable to serve read")
	}
	s.mtx.Lock()
	defer s.mtx.Unlock()
	return s.fsm.Read(key), nil
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
