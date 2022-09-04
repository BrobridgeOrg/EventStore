package eventstore

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble"
)

type Store struct {
	counter         Counter
	eventstore      *EventStore
	options         *Options
	name            string
	dbPath          string
	columnFamilies  map[string]*ColumnFamily
	snapshotLastSeq uint64

	subscriptions sync.Map
}

func NewStore(eventstore *EventStore, storeName string) (*Store, error) {

	store := &Store{
		eventstore:     eventstore,
		options:        eventstore.options,
		name:           storeName,
		dbPath:         filepath.Join(eventstore.options.DatabasePath, storeName),
		columnFamilies: make(map[string]*ColumnFamily),
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

	err := os.MkdirAll(store.dbPath, os.ModePerm)
	if err != nil {
		return err
	}

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

	for _, cf := range store.columnFamilies {
		cf.Close()
	}

	store.eventstore.UnregisterStore(store.name)
}

func (store *Store) assertColumnFamily(name string) (*ColumnFamily, error) {

	cf, ok := store.columnFamilies[name]
	if !ok {
		cf := NewColumnFamily(store, name)
		err := cf.Open()
		if err != nil {
			return nil, err
		}

		store.columnFamilies[name] = cf

		return cf, nil
	}

	return cf, nil
}

func (store *Store) GetColumnFamailyHandle(name string) (*ColumnFamily, error) {
	return store.getColumnFamailyHandle(name)
}

func (store *Store) getColumnFamailyHandle(name string) (*ColumnFamily, error) {

	cf, ok := store.columnFamilies[name]
	if !ok {
		return nil, fmt.Errorf("Not found \"%s\" column family", name)
	}

	return cf, nil
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
		return fmt.Errorf("Failed to initialize counter: %v", err)
	}

	iter := cfHandle.Db.NewIter(nil)
	defer iter.Close()

	counter := Counter(1)
	for iter.Last(); iter.Valid(); iter.Next() {
		lastSeq := BytesToUint64(iter.Key())
		counter.SetCount(lastSeq + 1)
	}

	store.counter = counter

	return nil
}

func (store *Store) Delete(seq uint64) error {

	cfHandle, err := store.GetColumnFamailyHandle("events")
	if err != nil {
		return err
	}

	// Preparing key-value
	key := Uint64ToBytes(seq)

	// Write
	err = cfHandle.Delete(key)
	if err != nil {
		return err
	}

	return nil
}

func (store *Store) Write(data []byte) (uint64, error) {

	cfHandle, err := store.GetColumnFamailyHandle("events")
	if err != nil {
		return 0, err
	}

	// Getting sequence
	seq := store.counter.Count()
	store.counter.Increase(1)

	// Preparing key-value
	key := Uint64ToBytes(seq)

	// Write
	err = cfHandle.Write(key, data)
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

func (store *Store) GetSnapshotLastSequence() uint64 {
	return atomic.LoadUint64((*uint64)(&store.snapshotLastSeq))
}

func (store *Store) GetLastSequence() uint64 {
	return store.counter.Count() - 1
}

func (store *Store) GetDurableState(durableName string) (uint64, error) {

	cfHandle, err := store.GetColumnFamailyHandle("states")
	if err != nil {
		return 0, err
	}

	// Write
	value, closer, err := cfHandle.Db.Get(StrToBytes(durableName))
	if err != nil {
		if err == pebble.ErrNotFound {
			return 0, nil
		}

		return 0, err
	}

	lastSeq := BytesToUint64(value)

	closer.Close()

	return lastSeq, nil
}

func (store *Store) UpdateDurableState(durableName string, lastSeq uint64) error {

	cfHandle, err := store.GetColumnFamailyHandle("states")
	if err != nil {
		return err
	}

	value := Uint64ToBytes(lastSeq)

	// Write
	err = cfHandle.Write(StrToBytes(durableName), value)
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

func (store *Store) createSubscription(fn StoreHandler, opts ...SubOpt) (*Subscription, error) {

	cfHandle, err := store.GetColumnFamailyHandle("events")
	if err != nil {
		return nil, err
	}

	//	iter := cfHandle.Db.NewIter(nil)

	// Create a new subscription entry
	sub := NewSubscription(store, cfHandle, fn, opts...)

	return sub, nil
}

func (store *Store) Subscribe(fn StoreHandler, opts ...SubOpt) (*Subscription, error) {

	sub, err := store.createSubscription(fn, opts...)
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
		return nil, err
	}

	total := 0
	offsetCounter := offset
	iter := cfHandle.Db.NewIter(nil)
	for iter.SeekGE(Uint64ToBytes(startAt)); iter.Valid(); iter.Next() {
		seq := BytesToUint64(iter.Key())

		if offsetCounter > 0 {
			offsetCounter--
			continue
		}

		data := make([]byte, len(iter.Value()))
		copy(data, iter.Value())

		// Create event
		event := NewEvent()
		event.Sequence = seq
		event.Data = data

		events = append(events, event)

		// limit
		total++
		if total == count {
			break
		}
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
