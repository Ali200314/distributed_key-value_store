package store

import (
	"io"
	"sync"

	"github.com/hashicorp/raft"
)

type Fsm struct {
	values map[string]string
	mtx    sync.Mutex
}

func (f *Fsm) Snapshot() (raft.FSMSnapshot, error) {
	//TODO implement me
	panic("implement me")
}

func (f *Fsm) Restore(snapshot io.ReadCloser) error {
	//TODO implement me
	panic("implement me")
}

func NewFSM() *Fsm {
	return &Fsm{}
}

func (f *Fsm) Apply(l *raft.Log) interface{} {
	return nil
}
