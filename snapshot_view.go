package eventstore

import (
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
	sv.nativeSnapshot = sv.store.db.NewSnapshot()

	return nil
}

func (sv *SnapshotView) Release() error {
	if sv.nativeSnapshot != nil {
		sv.nativeSnapshot.Close()
	}

	return nil
}

func (sv *SnapshotView) keyUpperBound(b []byte) []byte {
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

func (sv *SnapshotView) prefixIterOptions(prefix []byte) *pebble.IterOptions {
	return &pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: sv.keyUpperBound(prefix),
	}
}

func (sv *SnapshotView) Fetch(collection []byte, key []byte, offset uint64, count int) ([]*Record, error) {

	records := make([]*Record, 0, count)

	// Prepare snapshot key
	snapshotKey := genSnapshotKey(
		collection,
		key,
	)

	rk, err := sv.store.cfSnapshot.genRawKey(snapshotKey)
	if err != nil {
		return nil, err
	}

	prefix := rk[0 : len(rk)-len(key)]

	// Create Iterator
	iter := sv.nativeSnapshot.NewIter(sv.prefixIterOptions(prefix))

	// Seek
	iter.SeekGE(rk)
	offsetCounter := offset
	for i := 0; i < count && iter.Valid(); {

		if offsetCounter > 0 {
			offsetCounter--
			iter.Next()
			continue
		}

		i++

		// Getting key
		recordKey := make([]byte, len(iter.Key())-len(prefix))
		copy(recordKey, iter.Key()[len(prefix):])

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

func (sv *SnapshotView) Get(collection []byte, key []byte) ([]byte, error) {

	// Prepare snapshot key
	snapshotKey := genSnapshotKey(
		collection,
		key,
	)

	rk, err := sv.store.cfSnapshot.genRawKey(snapshotKey)
	if err != nil {
		return nil, err
	}

	v, closer, err := sv.nativeSnapshot.Get(rk)
	if err != nil {
		return nil, err
	}

	closer.Close()

	return v, nil
}
