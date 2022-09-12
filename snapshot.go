package eventstore

import (
	"sync"

	"github.com/cockroachdb/pebble"
)

type SnapshotRequest struct {
	Sequence uint64
	Store    *Store
	Data     []byte
	Batch    *pebble.Batch
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

	snapshotKey := genSnapshotKey(
		collection,
		key,
	)

	value, closer, err := request.Store.cfSnapshot.Get(snapshotKey)
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

	snapshotKey := genSnapshotKey(
		collection,
		key,
	)

	oldValue, closer, err := request.Store.cfSnapshot.Get(snapshotKey)
	if err != nil {
		if err != pebble.ErrNotFound {
			return err
		}
	}

	if closer != nil {
		defer closer.Close()
	}

	//fmt.Println(oldValue, value)

	err = request.Store.cfSnapshot.Write(nil, snapshotKey, fn(oldValue, value))
	if err != nil {
		return err
	}

	// Update snapshot state
	err = request.updateDurableState(nil, collection)
	if err != nil {
		return err
	}

	request.Store.requestSync()

	return nil
}

func (request *SnapshotRequest) updateDurableState(b *pebble.Batch, collection []byte) error {

	// Update snapshot state
	return request.Store.SetStateUint64(b, []byte("snapshot"), collection, []byte("lastseq"), request.Sequence)
}

func (request *SnapshotRequest) UpdateDurableState(b *pebble.Batch, collection []byte) error {
	return request.updateDurableState(b, collection)
}

func (request *SnapshotRequest) Delete(collection []byte, key []byte) error {

	snapshotKey := genSnapshotKey(
		collection,
		key,
	)

	err := request.Store.cfSnapshot.Delete(snapshotKey)
	if err != nil {
		return err
	}

	request.Store.requestSync()

	return nil
}

func (request *SnapshotRequest) write(collection []byte, key []byte, data []byte) error {

	err := request.Store.cfSnapshot.Write(nil, key, data)
	if err != nil {
		return err
	}

	// Update snapshot state
	err = request.updateDurableState(nil, collection)
	if err != nil {
		return err
	}

	request.Store.requestSync()

	return nil
}
