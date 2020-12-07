package eventstore

import (
	"errors"
	"fmt"

	"github.com/cfsghost/gosharding"
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
		err := ss.handle(req)
		if err != nil {
			fmt.Println(err)
		}
	}

	// Create shard with options
	ss.shard = gosharding.NewShard(options)

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
