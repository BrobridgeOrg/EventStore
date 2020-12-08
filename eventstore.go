package eventstore

import (
	"os"
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

func (eventstore *EventStore) GetStore(storeName string) (*Store, error) {

	if store, ok := eventstore.stores[storeName]; ok {
		return store, nil
	}

	store, err := NewStore(eventstore, storeName)
	if err != nil {
		return nil, err

	}

	eventstore.stores[storeName] = store

	return store, nil
}

func (eventstore *EventStore) TakeSnapshot(store *Store, seq uint64, data []byte) error {

	if eventstore.snapshot == nil {
		return nil
	}

	return eventstore.snapshot.Request(store, seq, data)
}
