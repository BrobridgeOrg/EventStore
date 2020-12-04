package eventstore

import (
	"os"
)

type EventStore struct {
	dbPath  string
	stores  map[string]*Store
	options *Options
}

func CreateEventStore(options *Options) (*EventStore, error) {

	err := os.MkdirAll(options.DatabasePath, os.ModePerm)
	if err != nil {
		return nil, err
	}

	return &EventStore{
		stores:  make(map[string]*Store),
		options: options,
	}, nil
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
