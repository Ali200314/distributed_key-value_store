package store

import (
	"encoding/json"
	"io"
	"log"
	"maps"
	"sync"

	"github.com/hashicorp/raft"
)

type command struct {
	Operation string
	Key       string
	Value     string
}
type Fsm struct {
	values map[string]string
	mtx    sync.Mutex
}

func NewFSM() *Fsm {
	return &Fsm{
		values: make(map[string]string),
	}
}

func (f *Fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	valuesCopy := make(map[string]string)
	maps.Copy(valuesCopy, f.values)
	return snapshotImpl{values: valuesCopy}, nil
}

func (f *Fsm) Restore(snapshot io.ReadCloser) error {
	v := make(map[string]string)
	err := json.NewDecoder(snapshot).Decode(&v)
	if err != nil {
		return err
	}
	f.values = v
	return nil
}

func (f *Fsm) Apply(l *raft.Log) interface{} {
	cmd := &command{}
	err := json.Unmarshal(l.Data, cmd)
	if err != nil {
		log.Fatal("unable to unmarshal log data")
	}

	f.mtx.Lock()
	defer f.mtx.Unlock()
	if cmd.Operation == _delete {
		delete(f.values, cmd.Key)
	} else if cmd.Operation == _set {
		f.values[cmd.Key] = cmd.Value
	} else {
		log.Fatal("unknown operation")
	}
	return nil
}

func (f *Fsm) Read(key string) string {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	return f.values[key]
}
