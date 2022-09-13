package eventstore

import (
	"bytes"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/cfsghost/gosharding"
	"github.com/cockroachdb/pebble"
)

func genSnapshotKey(collection []byte, key []byte) []byte {
	return bytes.Join([][]byte{
		collection,
		key,
	}, []byte("."))
}

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
		defer snapshotRequestPool.Put(req)

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

	b := req.Store.db.NewIndexedBatch()

	req.Batch = b

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

	err = b.Commit(pebble.NoSync)
	if err != nil {
		return err
	}

	req.Store.requestSync()

	return nil
}

func (ss *SnapshotController) handle(req *SnapshotRequest) error {

	if ss.handler == nil {
		return errors.New("No snapshot handler is set")
	}

	err := ss.handler(req)

	return err
}

func (ss *SnapshotController) updateSnapshotState(req *SnapshotRequest) error {

	// Update store property
	atomic.StoreUint64((*uint64)(&req.Store.state.snapshotLastSeq), req.Sequence)
	return req.Store.state.syncSnapshotLastSeq(req.Store, req.Batch)
}

func (ss *SnapshotController) SetHandler(fn func(*SnapshotRequest) error) {
	ss.handler = fn
}

func (ss *SnapshotController) Request(b *pebble.Batch, store *Store, seq uint64, data []byte) error {

	// Create a new snapshot request
	req := snapshotRequestPool.Get().(*SnapshotRequest)
	req.Store = store
	req.Sequence = seq
	req.Data = data
	req.Batch = nil

	ss.shard.PushKV(store.name, req)

	return nil
}

func (ss *SnapshotController) RecoverSnapshot(store *Store) error {

	// Loading last sequence of snapshot
	lastSeq := store.state.SnapshotLastSeq()

	// No need to recover snapshot
	if lastSeq >= store.state.LastSeq() {
		return nil
	}

	key := Uint64ToBytes(lastSeq)

	cur, err := store.cfEvent.List([]byte(""), key, &ListOptions{
		WithoutRawPrefix: true,
	})
	if err != nil {
		return err
	}

	for ; !cur.EOF(); cur.Next() {

		seq := BytesToUint64(cur.GetKey())

		if seq == lastSeq {
			continue
		}

		data := make([]byte, len(cur.GetData()))
		copy(data, cur.GetData())

		// Create a new snapshot request
		req := snapshotRequestPool.Get().(*SnapshotRequest)
		req.Store = store
		req.Sequence = seq
		req.Data = data
		req.Batch = nil

		// process
		err := ss.handleRequest(req)
		if err != nil {
			return err
		}

		// Release
		snapshotRequestPool.Put(req)

	}

	cur.Close()

	return nil
}
