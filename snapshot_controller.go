package eventstore

import (
	"errors"
	"fmt"

	"github.com/cfsghost/gosharding"
	"github.com/cockroachdb/pebble"
)

type SnapshotController struct {
	options *SnapshotOptions
	shard   *gosharding.Shard
	handler func(*SnapshotRequest) error
}

func NewSnapshotController(options *SnapshotOptions) *SnapshotController {
	ss := &SnapshotController{
		options: options,
	}
	ss.initialize()
	return ss
}

func (ss *SnapshotController) initialize() error {

	// Initializing shard
	options := gosharding.NewOptions()
	options.PipelineCount = ss.options.WorkerCount
	options.BufferSize = ss.options.BufferSize
	options.Handler = func(id int32, data interface{}) {

		req := data.(*SnapshotRequest)
		err := ss.handleRequest(req)
		if err != nil {
			fmt.Println(err)
			return
		}
	}

	// Create shard with options
	ss.shard = gosharding.NewShard(options)

	return nil
}

func (ss *SnapshotController) handleRequest(req *SnapshotRequest) error {

	err := ss.handle(req)
	if err != nil {
		fmt.Println(err)
		return err
	}

	// Update snapshot state with  the last sequence number
	err = ss.updateSnapshotState(req)
	if err != nil {
		fmt.Println(err)
		return err
	}

	return nil
}

func (ss *SnapshotController) handle(req *SnapshotRequest) error {

	if ss.handler == nil {
		return errors.New("No snapshot handler is set")
	}

	err := ss.handler(req)
	snapshotRequestPool.Put(req)

	return err
}

func (ss *SnapshotController) updateSnapshotState(req *SnapshotRequest) error {

	stateHandle, err := req.Store.GetColumnFamailyHandle("snapshot_states")
	if err != nil {
		return errors.New("Not found \"snapshot_states\" column family")
	}

	// Update snapshot state
	seqData := Uint64ToBytes(req.Sequence)
	err = stateHandle.Db.Set([]byte("_state"), seqData, pebble.NoSync)
	if err != nil {
		return err
	}

	return nil
}

func (ss *SnapshotController) SetHandler(fn func(*SnapshotRequest) error) {
	ss.handler = fn
}

func (ss *SnapshotController) Request(store *Store, seq uint64, data []byte) error {

	// Create a new snapshot request
	req := snapshotRequestPool.Get().(*SnapshotRequest)
	req.Store = store
	req.Sequence = seq
	req.Data = data

	ss.shard.PushKV(store.name, req)

	return nil
}

func (ss *SnapshotController) RecoverSnapshot(store *Store) error {

	stateHandle, err := store.GetColumnFamailyHandle("snapshot_states")
	if err != nil {
		return errors.New("Not found \"snapshot_states\" column family")
	}

	value, closer, err := stateHandle.Db.Get([]byte("_state"))
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil
		}

		return err
	}

	defer closer.Close()

	cfHandle, err := store.GetColumnFamailyHandle("events")
	if err != nil {
		return errors.New("Not found \"events\" column family")
	}

	iter := cfHandle.Db.NewIter(nil)
	iter.SeekGE(value)
	closer.Close()
	for ; iter.Valid(); iter.Next() {
		seq := BytesToUint64(iter.Key())

		data := make([]byte, len(iter.Value()))
		copy(data, iter.Value())

		// Create a new snapshot request
		req := snapshotRequestPool.Get().(*SnapshotRequest)
		req.Store = store
		req.Sequence = seq
		req.Data = data

		// process
		err := ss.handleRequest(req)
		if err != nil {
			return err
		}

		// Release
		snapshotRequestPool.Put(req)
	}

	iter.Close()

	return nil
}
