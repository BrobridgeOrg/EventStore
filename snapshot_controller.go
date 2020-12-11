package eventstore

import (
	"errors"
	"fmt"

	"github.com/cfsghost/gosharding"
	"github.com/tecbot/gorocksdb"
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
	err = req.Store.db.PutCF(req.Store.wo, stateHandle, []byte("_state"), seqData)
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

	value, err := store.db.GetCF(store.ro, stateHandle, []byte("_state"))
	if err != nil {
		return err
	}

	// Nothing to do recovery
	if value.Size() == 0 {
		value.Free()
		return nil
	}

	cfHandle, err := store.GetColumnFamailyHandle("events")
	if err != nil {
		return errors.New("Not found \"events\" column family")
	}

	// Getting events which is not yet handled
	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)
	iter := store.db.NewIteratorCF(ro, cfHandle)
	if iter.Err() != nil {
		value.Free()
		return iter.Err()
	}

	iter.Seek(value.Data())
	for ; iter.Valid(); iter.Next() {

		// Getting sequence number
		k := iter.Key()
		seq := BytesToUint64(k.Data())
		k.Free()

		// Create a new snapshot request
		v := iter.Value()
		req := snapshotRequestPool.Get().(*SnapshotRequest)
		req.Store = store
		req.Sequence = seq
		req.Data = v.Data()

		// process
		err := ss.handleRequest(req)
		if err != nil {
			value.Free()
			v.Free()
			return err
		}

		// Release
		v.Free()
		snapshotRequestPool.Put(req)
	}

	iter.Close()
	value.Free()

	return nil
}
