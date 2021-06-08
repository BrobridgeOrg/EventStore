package eventstore

import (
	"bytes"
	"errors"

	"github.com/cockroachdb/pebble"
)

type SnapshotView struct {
	store          *Store
	nativeSnapshot *pebble.Snapshot
}

func NewSnapshotView(store *Store) *SnapshotView {
	return &SnapshotView{
		store: store,
	}
}

func (sv *SnapshotView) Initialize() error {

	cfHandle, err := sv.store.GetColumnFamailyHandle("snapshot")
	if err != nil {
		return errors.New("Not found \"snapshot\" column family")
	}

	sv.nativeSnapshot = cfHandle.Db.NewSnapshot()

	return nil
}

func (sv *SnapshotView) Release() error {
	if sv.nativeSnapshot != nil {
		sv.nativeSnapshot.Close()
	}

	return nil
}

func (sv *SnapshotView) Fetch(collection []byte, key []byte, offset uint64, count int) ([]*Record, error) {

	records := make([]*Record, 0, count)

	keyUpperBound := func(b []byte) []byte {
		end := make([]byte, len(b))
		copy(end, b)
		for i := len(end) - 1; i >= 0; i-- {
			end[i] = end[i] + 1
			if end[i] != 0 {
				return end[:i+1]
			}
		}
		return nil // no upper-bound
	}

	prefixIterOptions := func(prefix []byte) *pebble.IterOptions {
		return &pebble.IterOptions{
			LowerBound: prefix,
			UpperBound: keyUpperBound(prefix),
		}
	}

	// Prepare snapshot key
	snapshotKey := bytes.Join([][]byte{
		collection,
		key,
	}, []byte("-"))
	prefix := snapshotKey[0 : len(snapshotKey)-len(key)]

	// Create Iterator
	iter := sv.nativeSnapshot.NewIter(prefixIterOptions(prefix))

	// Seek
	iter.SeekGE(snapshotKey)
	offsetCounter := offset
	for i := 0; i < count && iter.Valid(); i++ {

		// Getting key
		recordKey := make([]byte, len(iter.Key())-len(prefix))
		copy(recordKey, iter.Key()[len(prefix):])

		if offsetCounter > 0 {
			offsetCounter--
			iter.Next()
			continue
		}

		// Value
		data := make([]byte, len(iter.Value()))
		copy(data, iter.Value())

		// Preparing record
		record := NewRecord()
		record.Key = recordKey
		record.Data = data

		records = append(records, record)

		iter.Next()
	}

	iter.Close()

	return records, nil
}
