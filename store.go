package eventstore

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/tecbot/gorocksdb"
)

type Store struct {
	eventstore *EventStore
	options    *Options
	name       string
	db         *gorocksdb.DB
	cfHandles  map[string]*gorocksdb.ColumnFamilyHandle
	ro         *gorocksdb.ReadOptions
	wo         *gorocksdb.WriteOptions
	counter    Counter

	subscriptions sync.Map
}

func NewStore(eventstore *EventStore, storeName string) (*Store, error) {

	// Initializing options
	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)
	//	ro.SetTailing(true)
	wo := gorocksdb.NewDefaultWriteOptions()

	store := &Store{
		eventstore: eventstore,
		options:    eventstore.options,
		name:       storeName,
		cfHandles:  make(map[string]*gorocksdb.ColumnFamilyHandle),
		ro:         ro,
		wo:         wo,
	}

	err := store.openDatabase()
	if err != nil {
		return nil, err
	}

	err = store.initializeCounter()
	if err != nil {
		store.Close()
		return nil, err
	}

	// Initializing snapshot
	if eventstore.options.EnabledSnapshot {

		// Assert snapshot
		_, err := store.assertColumnFamily("snapshot")
		if err != nil {
			return nil, err
		}

		// Assert snapshot states
		_, err = store.assertColumnFamily("snapshot_states")
		if err != nil {
			return nil, err
		}

		// Recovery snapshot requests which is still pending
		err = store.eventstore.RecoverSnapshot(store)
		if err != nil {
			return store, err
		}
	}

	return store, nil
}

func (store *Store) openDatabase() error {

	dbpath := filepath.Join(store.options.DatabasePath, store.name)
	err := os.MkdirAll(dbpath, os.ModePerm)
	if err != nil {
		return err
	}

	// List column families
	cfNames, _ := gorocksdb.ListColumnFamilies(store.options.RocksdbOptions, dbpath)

	if len(cfNames) == 0 {
		cfNames = []string{"default"}
	}

	// Preparing options for column families
	cfOpts := make([]*gorocksdb.Options, len(cfNames))
	for i := range cfNames {
		cfOpts[i] = store.options.RocksdbOptions
	}

	// Open database
	db, cfHandles, err := gorocksdb.OpenDbColumnFamilies(store.options.RocksdbOptions, dbpath, cfNames, cfOpts)
	if err != nil {
		return err
	}

	for i, name := range cfNames {
		store.cfHandles[name] = cfHandles[i]
	}

	store.db = db

	err = store.initializeColumnFamily()
	if err != nil {
		return err
	}

	return nil
}

func (store *Store) Close() {

	store.subscriptions.Range(func(k, v interface{}) bool {
		sub := v.(*Subscription)
		sub.Close()
		return true
	})

	store.db.Close()
	store.eventstore.UnregisterStore(store.name)
}

func (store *Store) assertColumnFamily(name string) (*gorocksdb.ColumnFamilyHandle, error) {

	handle, ok := store.cfHandles[name]
	if !ok {
		handle, err := store.db.CreateColumnFamily(store.options.RocksdbOptions, name)
		if err != nil {
			return nil, err
		}

		store.cfHandles[name] = handle

		return handle, nil
	}

	return handle, nil
}

func (store *Store) initializeColumnFamily() error {

	// Assert events
	_, err := store.assertColumnFamily("events")
	if err != nil {
		return err
	}

	// Assert states
	_, err = store.assertColumnFamily("states")
	if err != nil {
		return err
	}

	return nil
}

func (store *Store) initializeCounter() error {

	cfHandle, err := store.GetColumnFamailyHandle("events")
	if err != nil {
		return errors.New("Not found \"events\" column family, so failed to initialize counter.")
	}

	// Initializing counter
	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)
	iter := store.db.NewIteratorCF(ro, cfHandle)
	defer iter.Close()

	// Find last key
	iter.SeekToLast()
	counter := Counter(1)
	if iter.Err() != nil {
		return iter.Err()
	}

	if iter.Valid() {
		key := iter.Key()
		lastSeq := BytesToUint64(key.Data())
		key.Free()
		counter.SetCount(lastSeq + 1)
	}

	store.counter = counter

	return nil
}

func (store *Store) GetColumnFamailyHandle(name string) (*gorocksdb.ColumnFamilyHandle, error) {

	cfHandle, ok := store.cfHandles[name]
	if !ok {
		return nil, fmt.Errorf("Not found \"%s\" column family", name)
	}

	return cfHandle, nil
}

func (store *Store) Write(data []byte) (uint64, error) {

	cfHandle, err := store.GetColumnFamailyHandle("events")
	if err != nil {
		return 0, errors.New("Not found \"events\" column family")
	}

	// Getting sequence
	seq := store.counter.Count()
	store.counter.Increase(1)

	// Preparing key-value
	key := Uint64ToBytes(seq)

	// Write
	err = store.db.PutCF(store.wo, cfHandle, key, data)
	if err != nil {
		return 0, err
	}

	// Take snapshot
	store.eventstore.TakeSnapshot(store, seq, data)

	// Dispatch event to subscribers
	store.DispatchEvent()

	return seq, nil
}

func (store *Store) DispatchEvent() {

	// Publish event to all of subscription which is waiting for
	store.subscriptions.Range(func(k, v interface{}) bool {
		sub := v.(*Subscription)
		sub.Trigger()
		return true
	})
}

func (store *Store) GetLastSequence() uint64 {
	return store.counter.Count() - 1
}

func (store *Store) GetDurableState(durableName string) (uint64, error) {

	cfHandle, err := store.GetColumnFamailyHandle("states")
	if err != nil {
		return 0, errors.New("Not found \"states\" column family")
	}

	// Write
	value, err := store.db.GetCF(store.ro, cfHandle, StrToBytes(durableName))
	if err != nil {
		return 0, err
	}

	if !value.Exists() {
		return 0, nil
	}

	lastSeq := BytesToUint64(value.Data())
	value.Free()

	return lastSeq, nil
}

func (store *Store) UpdateDurableState(durableName string, lastSeq uint64) error {

	cfHandle, err := store.GetColumnFamailyHandle("states")
	if err != nil {
		return errors.New("Not found \"states\" column family")
	}

	value := Uint64ToBytes(lastSeq)

	// Write
	err = store.db.PutCF(store.wo, cfHandle, StrToBytes(durableName), value)
	if err != nil {
		return err
	}

	return nil
}

func (store *Store) registerSubscription(sub *Subscription) error {
	store.subscriptions.Store(sub, sub)
	return nil
}

func (store *Store) unregisterSubscription(sub *Subscription) error {
	store.subscriptions.Delete(sub)
	return nil
}

func (store *Store) createSubscription(durableName string, startAt uint64, fn StoreHandler) (*Subscription, error) {

	cfHandle, err := store.GetColumnFamailyHandle("events")
	if err != nil {
		return nil, errors.New("Not found \"events\" column family")
	}

	// Initializing iterator
	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)
	ro.SetTailing(true)
	iter := store.db.NewIteratorCF(ro, cfHandle)
	if iter.Err() != nil {
		return nil, iter.Err()
	}

	// Create a new subscription entry
	sub := NewSubscription(store, durableName, startAt, iter, fn)

	return sub, nil
}

func (store *Store) Subscribe(durableName string, startAt uint64, fn StoreHandler) (*Subscription, error) {

	sub, err := store.createSubscription(durableName, startAt, fn)
	if err != nil {
		return nil, err
	}

	// Register subscription
	store.registerSubscription(sub)

	// Start watching
	go store.watch(sub)

	return sub, nil
}

func (store *Store) Fetch(startAt uint64, offset uint64, count int) ([]*Event, error) {

	events := make([]*Event, 0, count)

	if startAt+offset >= store.counter.Count() {
		return events, nil
	}

	cfHandle, err := store.GetColumnFamailyHandle("events")
	if err != nil {
		return nil, errors.New("Not found \"events\" column family")
	}

	// Initializing iterator
	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)
	ro.SetTailing(true)
	iter := store.db.NewIteratorCF(ro, cfHandle)
	if iter.Err() != nil {
		return nil, iter.Err()
	}

	iter.Seek(Uint64ToBytes(startAt))

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

	iter.Close()

	return events, nil
}

func (store *Store) CreateSnapshotView() *SnapshotView {
	return NewSnapshotView(store)
}

func (store *Store) watch(sub *Subscription) {

	// Start watching event store
	sub.Watch()

	// Release
	store.unregisterSubscription(sub)
}
