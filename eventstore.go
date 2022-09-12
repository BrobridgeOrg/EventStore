package eventstore

import (
	"errors"
	"os"

	"github.com/cockroachdb/pebble"
)

var (
	ErrRecordNotFound = errors.New("EventStore: record not found")
)

type EventStore struct {
	options  *Options
	dbPath   string
	stores   map[string]*Store
	snapshot *SnapshotController
}

func CreateEventStore(options *Options) (*EventStore, error) {

	err := os.MkdirAll(options.DatabasePath, os.ModePerm)
	if err != nil {
		return nil, err
	}

	eventStore := &EventStore{
		stores:  make(map[string]*Store),
		options: options,
	}

	if options.EnabledSnapshot {
		eventStore.initializeSnapshotController()
	}

	return eventStore, nil
}

func (eventstore *EventStore) initializeSnapshotController() error {
	eventstore.snapshot = NewSnapshotController(eventstore.options.SnapshotOptions)
	return nil
}

func (eventstore *EventStore) UnregisterStore(name string) {
	delete(eventstore.stores, name)
}

func (eventstore *EventStore) Close() {
	for _, store := range eventstore.stores {
		store.Close()
	}

	eventstore.stores = make(map[string]*Store)
}

func (eventstore *EventStore) SetSnapshotHandler(fn func(*SnapshotRequest) error) {

	if eventstore.snapshot == nil {
		return
	}

	eventstore.snapshot.SetHandler(fn)
}

func (eventstore *EventStore) GetStore(storeName string, opts ...StoreOpt) (*Store, error) {

	if store, ok := eventstore.stores[storeName]; ok {
		return store, nil
	}

	store, err := NewStore(eventstore, storeName, opts...)
	if err != nil {
		return nil, err

	}

	eventstore.stores[storeName] = store

	return store, nil
}

func (eventstore *EventStore) TakeSnapshot(b *pebble.Batch, store *Store, seq uint64, data []byte) error {

	if eventstore.snapshot == nil {
		return nil
	}

	return eventstore.snapshot.Request(b, store, seq, data)
}

func (eventstore *EventStore) RecoverSnapshot(store *Store) error {

	if eventstore.snapshot == nil {
		return nil
	}

	return eventstore.snapshot.RecoverSnapshot(store)
}
