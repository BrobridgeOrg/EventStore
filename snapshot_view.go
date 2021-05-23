package eventstore

import (
	"bytes"
	"errors"

	"github.com/tecbot/gorocksdb"
)

type SnapshotView struct {
	store          *Store
	nativeSnapshot *gorocksdb.Snapshot
}

func NewSnapshotView(store *Store) *SnapshotView {
	return &SnapshotView{
		store: store,
	}
}

func (sv *SnapshotView) Initialize() error {

	sv.nativeSnapshot = sv.store.db.NewSnapshot()

	return nil
}

func (sv *SnapshotView) Release() error {
	if sv.nativeSnapshot != nil {
		sv.store.db.ReleaseSnapshot(sv.nativeSnapshot)
	}

	return nil
}

func (sv *SnapshotView) Fetch(collection []byte, key []byte, offset uint64, count int) ([]*Event, error) {

	events := make([]*Event, 0, count)

	cfHandle, err := sv.store.GetColumnFamailyHandle("snapshot")
	if err != nil {
		return nil, errors.New("Not found \"snapshot\" column family")
	}

	// Initializing iterator
	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)
	ro.SetTailing(true)
	ro.SetSnapshot(sv.nativeSnapshot)
	iter := sv.store.db.NewIteratorCF(ro, cfHandle)
	if iter.Err() != nil {
		return nil, iter.Err()
	}

	snapshotKey := bytes.Join([][]byte{
		collection,
		key,
	}, []byte("-"))

	iter.Seek(snapshotKey)

	offsetCounter := offset
	for i := 0; i < count && iter.Valid(); i++ {

		// Getting sequence number
		key := iter.Key()
		seq := BytesToUint64(key.Data())
		key.Free()

		if offsetCounter > 0 {
			offsetCounter--
			iter.Next()
			continue
		}

		// Value
		value := iter.Value()
		data := make([]byte, len(value.Data()))
		copy(data, value.Data())
		value.Free()

		// Create event
		event := NewEvent()
		event.Sequence = seq
		event.Data = data

		events = append(events, event)

		iter.Next()
	}

	return events, nil
}
