package eventstore

import (
	"bytes"
	"errors"
	"io"

	"github.com/cockroachdb/pebble"
)

var (
	ErrStateEntryNotFound = errors.New("store: state entry not found")
)

func (store *Store) genStateKey(class []byte, group []byte, prop []byte) []byte {

	return bytes.Join([][]byte{
		class,
		group,
		prop,
	}, []byte("."))

}

func (store *Store) putState(b *pebble.Batch, key []byte, value []byte) error {

	err := store.cfState.Write(b, key, value)
	if err != nil {
		return err
	}

	return nil
}

func (store *Store) getState(key []byte) ([]byte, io.Closer, error) {

	value, closer, err := store.cfState.Get(nil, key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, nil, ErrStateEntryNotFound
		}

		return nil, nil, err
	}

	return value, closer, nil
}

func (store *Store) deleteState(b *pebble.Batch, key []byte) error {

	err := store.cfState.Delete(b, key)
	if err != nil {
		return err
	}

	return nil
}

func (store *Store) SetStateBytes(b *pebble.Batch, class []byte, group []byte, key []byte, value []byte) error {

	k := store.genStateKey(class, group, key)

	return store.putState(b, k, value)
}

func (store *Store) GetStateBytes(class []byte, group []byte, key []byte) ([]byte, error) {

	k := store.genStateKey(class, group, key)

	value, closer, err := store.getState(k)
	if err != nil {
		return nil, err
	}

	data := make([]byte, len(value))
	copy(data, value)

	closer.Close()

	return data, nil
}

func (store *Store) DeleteState(b *pebble.Batch, class []byte, group []byte, key []byte) error {

	k := store.genStateKey(class, group, key)

	return store.deleteState(b, k)
}

func (store *Store) ListStates(class []byte, group []byte, key []byte) (*Cursor, error) {

	targetKey := append(bytes.Join([][]byte{
		class,
		group,
		key,
	}, []byte(".")))

	return store.cfState.List([]byte(""), targetKey, &ListOptions{
		WithoutRawPrefix: true,
	})
}

func (store *Store) SetStateInt64(b *pebble.Batch, class []byte, group []byte, key []byte, value int64) error {

	k := store.genStateKey(class, group, key)

	data := Int64ToBytes(value)

	return store.putState(b, k, data)
}

func (store *Store) GetStateInt64(class []byte, group []byte, key []byte) (int64, error) {

	k := store.genStateKey(class, group, key)

	value, closer, err := store.getState(k)
	if err != nil {
		return 0, err
	}

	data := BytesToInt64(value)

	closer.Close()

	return data, nil
}

func (store *Store) SetStateUint64(b *pebble.Batch, class []byte, group []byte, key []byte, value uint64) error {

	k := store.genStateKey(class, group, key)

	data := Uint64ToBytes(value)

	return store.putState(b, k, data)
}

func (store *Store) GetStateUint64(class []byte, group []byte, key []byte) (uint64, error) {

	k := store.genStateKey(class, group, key)

	value, closer, err := store.getState(k)
	if err != nil {
		return 0, err
	}

	data := BytesToUint64(value)

	closer.Close()

	return data, nil
}

func (store *Store) SetStateFloat64(b *pebble.Batch, class []byte, group []byte, key []byte, value float64) error {

	k := store.genStateKey(class, group, key)

	data := Float64ToBytes(value)

	return store.putState(b, k, data)
}

func (store *Store) GetStateFloat64(class []byte, group []byte, key []byte) (float64, error) {

	k := store.genStateKey(class, group, key)

	value, closer, err := store.getState(k)
	if err != nil {
		return 0, err
	}

	data := BytesToFloat64(value)

	closer.Close()

	return data, nil
}

func (store *Store) SetStateString(b *pebble.Batch, class []byte, group []byte, key []byte, value string) error {

	k := store.genStateKey(class, group, key)

	data := StrToBytes(value)

	return store.putState(b, k, data)
}

func (store *Store) GetStateString(class []byte, group []byte, key []byte) (string, error) {

	k := store.genStateKey(class, group, key)

	value, closer, err := store.getState(k)
	if err != nil {
		return "", err
	}

	data := make([]byte, len(value))
	copy(data, value)

	closer.Close()

	return BytesToString(data), nil
}
