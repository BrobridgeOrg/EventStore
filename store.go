package eventstore

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
)

var (
	ErrSnapshotNotEnabled = errors.New("EventStore: snapshot not enabled")
)

var (
	PrefixState        = []byte("s")
	PrefixEvent        = []byte("e")
	PrefixSnapshotData = []byte("d")
)

type StoreOpt func(s *Store)

type Store struct {
	counter    Counter
	eventstore *EventStore
	options    *Options
	name       string
	dbPath     string
	db         *pebble.DB
	batch      *pebble.Batch

	// column families
	cfState    *ColumnFamily
	cfEvent    *ColumnFamily
	cfSnapshot *ColumnFamily

	// schedular for synchronization
	maxSyncInterval time.Duration
	isScheduled     uint32
	timer           *time.Timer
	closed          chan struct{}

	// snapshot
	enabledSnapshot bool
	snapshotLastSeq uint64

	subscriptions sync.Map
}

func NewStore(eventstore *EventStore, storeName string, opts ...StoreOpt) (*Store, error) {

	store := &Store{
		eventstore:      eventstore,
		options:         eventstore.options,
		name:            storeName,
		dbPath:          filepath.Join(eventstore.options.DatabasePath, storeName),
		enabledSnapshot: eventstore.options.EnabledSnapshot,
		maxSyncInterval: time.Second * 10,
		closed:          make(chan struct{}),
	}

	for _, opt := range opts {
		opt(store)
	}

	if store.enabledSnapshot && !eventstore.options.EnabledSnapshot {
		return nil, ErrSnapshotNotEnabled
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
	if store.enabledSnapshot {

		// Recovery snapshot requests which is still pending
		err = store.eventstore.RecoverSnapshot(store)
		if err != nil {
			return store, err
		}
	}

	return store, nil
}

func WithSnapshot(enabled bool) StoreOpt {
	return func(s *Store) {
		s.enabledSnapshot = enabled
	}
}

func WithMaxSyncInterval(interval time.Duration) StoreOpt {
	return func(s *Store) {
		s.maxSyncInterval = interval
	}
}

func (store *Store) openDatabase() error {

	err := os.MkdirAll(store.dbPath, os.ModePerm)
	if err != nil {
		return err
	}

	// Preparing database options
	opts := &pebble.Options{
		//		DisableWAL:    true,
		MaxOpenFiles:  -1,
		LBaseMaxBytes: 512 << 20,
	}

	// Initialize 4 levels
	opts.Levels = make([]pebble.LevelOptions, 8)
	for i := range opts.Levels {
		l := &opts.Levels[i]

		// Level 0
		l.Compression = pebble.SnappyCompression
		l.BlockSize = 32 << 10      // 32MB
		l.TargetFileSize = 64 << 20 // 64MB

		if i > 0 {
			l.Compression = pebble.SnappyCompression
			l.TargetFileSize = opts.Levels[i-1].TargetFileSize * 2
		}

		opts.Levels[i].EnsureDefaults()
	}

	opts.EnsureDefaults()

	db, err := pebble.Open(store.dbPath, opts)
	if err != nil {
		return err
	}

	store.db = db
	store.batch = db.NewBatch()

	// Initializing scheduler
	store.timer = time.NewTimer(store.maxSyncInterval)
	store.timer.Stop()

	go store.sync()

	// Initializing column families
	store.cfState = NewColumnFamily(store, "state", PrefixState)
	store.cfEvent = NewColumnFamily(store, "event", PrefixEvent)
	store.cfSnapshot = NewColumnFamily(store, "snapshot", PrefixSnapshotData)

	return nil
}

func (store *Store) sync() {

	store.timer.Reset(time.Second * 10)

	for {

		select {
		case <-store.timer.C:
			store.db.LogData(nil, pebble.Sync)

			store.timer.Stop()
			store.timer.Reset(time.Second * 10)

			atomic.StoreUint32(&store.isScheduled, 0)
		case <-store.closed:
			store.timer.Stop()
			close(store.closed)
			return
		}
	}
}

func (store *Store) requestSync() {

	if atomic.LoadUint32(&store.isScheduled) != 0 {
		return
	}

	atomic.StoreUint32(&store.isScheduled, 1)

	// reset timer to perform sync in 100ms
	store.timer.Stop()
	store.timer.Reset(time.Millisecond * 100)
}

func (store *Store) close() error {
	store.closed <- struct{}{}
	store.db.LogData(nil, pebble.Sync)
	return store.db.Close()
}

func (store *Store) Close() {

	store.eventstore.UnregisterStore(store.name)

	store.subscriptions.Range(func(k, v interface{}) bool {
		sub := v.(*Subscription)
		sub.Close()
		return true
	})

	store.close()

	store.cfState = nil
	store.cfEvent = nil
	store.cfSnapshot = nil
}

func (store *Store) initializeCounter() error {

	// Getting last sequence from state store
	lastSeq, err := store.GetStateUint64([]byte("store"), []byte("event"), []byte("lastseq"))
	if err != nil {
		if err == ErrStateEntryNotFound {
			store.counter = Counter(1)
			return nil
		}

		return err
	}

	store.counter = Counter(lastSeq + 1)

	return nil
}

func (store *Store) updateLastSeq(b *pebble.Batch, seq uint64) error {

	err := store.SetStateUint64(b, []byte("store"), []byte("event"), []byte("lastseq"), seq)
	if err != nil {
		return err
	}

	return nil
}

func (store *Store) createNewSeq() uint64 {

	// Getting sequence
	seq := store.counter.Count()
	store.counter.Increase(1)

	return seq
}

func (store *Store) Get(seq uint64) ([]byte, error) {

	// Preparing key-value
	key := Uint64ToBytes(seq)

	// Write
	value, closer, err := store.cfEvent.Get(key)
	if err != nil {
		return nil, err
	}

	data := make([]byte, len(value))
	copy(data, value)

	closer.Close()

	return data, nil
}

func (store *Store) Delete(seq uint64) error {

	// Preparing key-value
	key := Uint64ToBytes(seq)

	// Write
	err := store.cfEvent.Delete(key)
	if err != nil {
		return err
	}

	store.requestSync()

	return nil
}

func (store *Store) Write(data []byte) (uint64, error) {

	// Getting sequence
	seq := store.createNewSeq()

	// Preparing key-value
	key := Uint64ToBytes(seq)

	b := store.db.NewBatch()

	// Write
	err := store.cfEvent.Write(b, key, data)
	if err != nil {
		return 0, err
	}

	// update seq
	err = store.updateLastSeq(b, seq)
	if err != nil {
		return 0, err
	}

	b.Commit(pebble.NoSync)

	// Take snapshot
	store.eventstore.TakeSnapshot(nil, store, seq, data)

	store.requestSync()

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
	return store.GetStateUint64([]byte("durable"), StrToBytes(durableName), []byte("lastseq"))
}

func (store *Store) UpdateDurableState(b *pebble.Batch, durableName string, lastSeq uint64) error {
	return store.SetStateUint64(b, []byte("durable"), StrToBytes(durableName), []byte("lastseq"), lastSeq)
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

	// Create a new subscription entry
	sub := NewSubscription(store, store.cfEvent, fn, opts...)

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

	startKey := Uint64ToBytes(startAt)
	cur, err := store.cfEvent.List([]byte(""), startKey, &ListOptions{
		WithoutRawPrefix: true,
	})
	if err != nil {
		return events, err
	}

	total := 0
	offsetCounter := offset
	for ; !cur.EOF(); cur.Next() {

		seq := BytesToUint64(cur.GetKey())

		if offsetCounter > 0 {
			offsetCounter--
			continue
		}

		data := make([]byte, len(cur.GetData()))
		copy(data, cur.GetData())

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

	cur.Close()

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
