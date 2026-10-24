package store

import (
	"encoding/json"

	"github.com/hashicorp/raft"
)

type snapshotImpl struct {
	values map[string]string
}

func (s snapshotImpl) Persist(sink raft.SnapshotSink) error {
	var err error
	defer func() {
		if err != nil {
			sink.Cancel()
		}
	}()
	bytes, err := json.Marshal(s.values)
	if err != nil {
		return err
	}
	_, err = sink.Write(bytes)
	if err != nil {
		return err
	}
	err = sink.Close()
	return err
}

func (s snapshotImpl) Release() {
}
