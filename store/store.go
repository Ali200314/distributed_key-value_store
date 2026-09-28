package store

import (
	"errors"

	"distributedkvstore/consistency"

	"github.com/hashicorp/raft"
)

type Store interface {
	Create(string, int) error
	Read(string) (int, error)
	Delete(string) error
	Update(string, int) error
}

type store struct {
	raft        *raft.Raft
	consistency consistency.Consistency
}

func (s *store) Create(key string, val int) error {
	if s.raft.State() != raft.Leader {
		return errors.New("follower unable to create")
	}
}

func (s *store) Read(key string) (int, error) {
	if s.raft.State() != raft.Leader && consistency.ReadConsistency() != followerRead {
		return 0, errors.New("follower unable to serve read")
	}
}

func (s *store) Delete(key string) error {
	if s.raft.State() != raft.Leader {
		return errors.New("follower unable to delete")
	}
}

func (s *store) Update(key string, val int) error {
	if s.raft.State() != raft.Leader {
		return errors.New("follower unable to update")
	}
}
