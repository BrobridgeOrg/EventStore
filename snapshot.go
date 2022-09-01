package eventstore

import (
	"bytes"
	"sync"

	"github.com/cockroachdb/pebble"
)

type SnapshotRequest struct {
	Sequence uint64
	Store    *Store
	Data     []byte
}

var snapshotRequestPool = sync.Pool{
	New: func() interface{} {
		return NewSnapshotRequest()
	},
}

func NewSnapshotRequest() *SnapshotRequest {
	return &SnapshotRequest{}
}

func (request *SnapshotRequest) Get(collection []byte, key []byte) ([]byte, error) {

	cfHandle, err := request.Store.GetColumnFamailyHandle("snapshot")
	if err != nil {
		return nil, err
	}

	snapshotKey := bytes.Join([][]byte{
		collection,
		key,
	}, []byte("-"))

	value, closer, err := cfHandle.Db.Get(snapshotKey)
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, ErrRecordNotFound
		}

		return nil, err
	}

	data := make([]byte, len(value))
	copy(data, value)

	closer.Close()

	return data, nil
}

func (request *SnapshotRequest) Upsert(collection []byte, key []byte, value []byte, fn func([]byte, []byte) []byte) error {

	cfHandle, err := request.Store.GetColumnFamailyHandle("snapshot")
	if err != nil {
		return err
	}

	snapshotKey := bytes.Join([][]byte{
		collection,
		key,
	}, []byte("-"))

	oldValue, closer, err := cfHandle.Db.Get(snapshotKey)
	if err != nil {
		if err != pebble.ErrNotFound {
			return err
		}
	}

	if closer != nil {
		defer closer.Close()
	}

	//fmt.Println(oldValue, value)

	err = cfHandle.Db.Set(snapshotKey, fn(oldValue, value), pebble.NoSync)
	if err != nil {
		return err
	}

	// Update snapshot state
	err = request.updateDurableState(nil, collection)
	if err != nil {
		return err
	}

	return nil
}

func (request *SnapshotRequest) updateDurableState(batch *pebble.Batch, collection []byte) error {

	stateHandle, err := request.Store.GetColumnFamailyHandle("snapshot_states")
	if err != nil {
		return err
	}

	// Update snapshot state
	seqData := Uint64ToBytes(request.Sequence)
	lastSequenceKey := bytes.Join([][]byte{
		collection,
		[]byte("seq"),
	}, []byte("-"))

	if batch == nil {
		err = stateHandle.Db.Set(lastSequenceKey, seqData, pebble.NoSync)
		if err != nil {
			return err
		}
	} else {
		batch.Set(lastSequenceKey, seqData, pebble.NoSync)
	}

	return nil
}

func (request *SnapshotRequest) UpdateDurableState(collection []byte) error {
	return request.updateDurableState(nil, collection)
}

func (request *SnapshotRequest) Delete(collection []byte, key []byte) error {

	cfHandle, err := request.Store.GetColumnFamailyHandle("snapshot")
	if err != nil {
		return err
	}

	snapshotKey := bytes.Join([][]byte{
		collection,
		key,
	}, []byte("-"))

	err = cfHandle.Db.Delete(snapshotKey, pebble.NoSync)
	if err != nil {
		return err
	}

	return nil
}

func (request *SnapshotRequest) write(collection []byte, key []byte, data []byte) error {

	cfHandle, err := request.Store.GetColumnFamailyHandle("snapshot")
	if err != nil {
		return err
	}

	err = cfHandle.Db.Set(key, data, pebble.NoSync)
	if err != nil {
		return err
	}

	// Update snapshot state
	err = request.updateDurableState(nil, collection)
	if err != nil {
		return err
	}

	return nil
}
