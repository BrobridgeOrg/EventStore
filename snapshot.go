package eventstore

import (
	"bytes"
	"errors"
	"sync"
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

	value, err := request.Store.db.GetCF(request.Store.ro, cfHandle, snapshotKey)
	if err != nil {
		return nil, err
	}

	data := make([]byte, len(value.Data()))
	copy(data, value.Data())
	value.Free()

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

	value, err := request.Store.db.GetCF(request.Store.ro, cfHandle, snapshotKey)
	if err != nil {
		return err
	}

	// Not found so insert a new record
	if value.Size() == 0 {
		value.Free()
		return request.write(collection, snapshotKey, request.Data)
	}

	// Update original data
	newData, err := fn(value.Data())
	value.Free()
	if err != nil {
		return err
	}

	return request.write(collection, snapshotKey, newData)
}

func (request *SnapshotRequest) Delete(collection []byte, key []byte) error {

	cfHandle, err := request.Store.GetColumnFamailyHandle("snapshot")
	if err != nil {
		return errors.New("Not found \"snapshot\" column family")
	}

	stateHandle, err := request.Store.GetColumnFamailyHandle("snapshot_states")
	if err != nil {
		return errors.New("Not found \"snapshot_states\" column family")
	}

	err = request.Store.db.DeleteCF(request.Store.wo, cfHandle, key)
	if err != nil {
		return err
	}

	// Update snapshot state
	seqData := Uint64ToBytes(request.Sequence)
	lastSequenceKey := bytes.Join([][]byte{
		collection,
		[]byte("seq"),
	}, []byte("-"))
	err = request.Store.db.PutCF(request.Store.wo, stateHandle, lastSequenceKey, seqData)
	if err != nil {
		return err
	}

	return nil
}

func (request *SnapshotRequest) write(collection []byte, key []byte, data []byte) error {

	cfHandle, err := request.Store.GetColumnFamailyHandle("snapshot")
	if err != nil {
		return errors.New("Not found \"snapshot\" column family")
	}

	stateHandle, err := request.Store.GetColumnFamailyHandle("snapshot_states")
	if err != nil {
		return errors.New("Not found \"snapshot_states\" column family")
	}

	// Write to database
	err = request.Store.db.PutCF(request.Store.wo, cfHandle, key, data)
	if err != nil {
		return err
	}

	// Update snapshot state
	seqData := Uint64ToBytes(request.Sequence)
	lastSequenceKey := bytes.Join([][]byte{
		collection,
		[]byte("seq"),
	}, []byte("-"))
	err = request.Store.db.PutCF(request.Store.wo, stateHandle, lastSequenceKey, seqData)
	if err != nil {
		return err
	}

	return nil
}
