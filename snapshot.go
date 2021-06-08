package eventstore

import (
	"bytes"
	"errors"
	"sync"

	"github.com/cockroachdb/pebble"
)

type SnapshotRequest struct {
	Store    *Store
	Sequence uint64
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
		return nil, errors.New("Not found \"snapshot\" column family")
	}

	snapshotKey := bytes.Join([][]byte{
		collection,
		key,
	}, []byte("-"))

	value, closer, err := cfHandle.Db.Get(snapshotKey)
	if err != nil {
		return nil, err
	}

	data := make([]byte, len(value))
	copy(data, value)

	closer.Close()

	return data, nil
}

func (request *SnapshotRequest) Upsert(collection []byte, key []byte, fn func(origin []byte) ([]byte, error)) error {

	cfHandle, err := request.Store.GetColumnFamailyHandle("snapshot")
	if err != nil {
		return errors.New("Not found \"snapshot\" column family")
	}

	snapshotKey := bytes.Join([][]byte{
		collection,
		key,
	}, []byte("-"))

	value, closer, err := cfHandle.Db.Get(snapshotKey)
	if err != nil {

		// Not found so insert a new record
		if err == pebble.ErrNotFound {
			return request.write(collection, snapshotKey, request.Data)
		}

		return err
	}

	// Update original data
	newData, err := fn(value)
	closer.Close()
	if err != nil {
		return err
	}

	return request.write(collection, snapshotKey, newData)
}

func (request *SnapshotRequest) updateDurableState(batch *pebble.Batch, collection []byte) error {

	stateHandle, err := request.Store.GetColumnFamailyHandle("snapshot_states")
	if err != nil {
		return errors.New("Not found \"snapshot_states\" column family")
	}

	// Update snapshot state
	seqData := Uint64ToBytes(request.Sequence)
	lastSequenceKey := bytes.Join([][]byte{
		collection,
		[]byte("seq"),
	}, []byte("-"))

	if batch == nil {
		err = stateHandle.Db.Set(lastSequenceKey, seqData, pebble.Sync)
		if err != nil {
			return err
		}
	} else {
		batch.Set(lastSequenceKey, seqData, nil)
	}

	return nil
}

func (request *SnapshotRequest) UpdateDurableState(collection []byte) error {
	return request.updateDurableState(nil, collection)
}

func (request *SnapshotRequest) Delete(collection []byte, key []byte) error {

	cfHandle, err := request.Store.GetColumnFamailyHandle("snapshot")
	if err != nil {
		return errors.New("Not found \"snapshot\" column family")
	}

	batch := cfHandle.Db.NewBatch()
	batch.Delete(key, nil)

	// Update snapshot state
	err = request.updateDurableState(batch, collection)
	if err != nil {
		batch.Close()
		return err
	}

	// Write to database
	err = cfHandle.Db.Apply(batch, nil)
	if err != nil {
		batch.Close()
		return err
	}

	batch.Close()

	return nil
}

func (request *SnapshotRequest) write(collection []byte, key []byte, data []byte) error {

	cfHandle, err := request.Store.GetColumnFamailyHandle("snapshot")
	if err != nil {
		return errors.New("Not found \"snapshot\" column family")
	}

	batch := cfHandle.Db.NewBatch()
	batch.Set(key, data, nil)

	// Update snapshot state
	err = request.updateDurableState(batch, collection)
	if err != nil {
		batch.Close()
		return err
	}

	// Write to database
	err = cfHandle.Db.Apply(batch, nil)
	if err != nil {
		batch.Close()
		return err
	}

	batch.Close()

	return nil
}
