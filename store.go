package eventstore

import (
	"bytes"
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

var (
	StateClassStore    = []byte("s")
	StateGroupEvent    = []byte("e")
	StateGroupSnapshot = []byte("s")
)

var (
	StatePathEventRev        = bytes.Join([][]byte{StateClassStore, StateGroupEvent, []byte("rev")}, []byte("."))
	StatePathEventLastSeq    = bytes.Join([][]byte{StateClassStore, StateGroupEvent, []byte("lastSeq")}, []byte("."))
	StatePathEventCount      = bytes.Join([][]byte{StateClassStore, StateGroupEvent, []byte("count")}, []byte("."))
	StatePathSnapshotLastSeq = bytes.Join([][]byte{StateClassStore, StateGroupSnapshot, []byte("lastSeq")}, []byte("."))
	StatePathSnapshotCount   = bytes.Join([][]byte{StateClassStore, StateGroupSnapshot, []byte("count")}, []byte("."))
)

type StoreState struct {
	rev             Counter
	count           Counter
	lastSeq         Counter
	snapshotCount   Counter
	snapshotLastSeq uint64
}

func (ss *StoreState) Rev() uint64 {
	return ss.rev.Count()
}

func (ss *StoreState) Count() uint64 {
	return ss.count.Count()
}

func (ss *StoreState) LastSeq() uint64 {
	return ss.lastSeq.Count()
}

func (ss *StoreState) SnapshotCount() uint64 {
	return ss.snapshotCount.Count()
}

func (ss *StoreState) SnapshotLastSeq() uint64 {
	return ss.snapshotLastSeq
}

func (ss *StoreState) loadSnapshotCount(s *Store) error {

	sc, err := s.GetStateUint64(StateClassStore, StateGroupSnapshot, []byte("count"))
	if err != nil {
		if err != ErrStateEntryNotFound {
			return err
		}

		sc = 0
	}

	ss.snapshotCount.SetCount(sc)

	return nil
}

func (ss *StoreState) loadSnapshotLastSeq(s *Store) error {

	lseq, err := s.GetStateUint64(StateClassStore, StateGroupSnapshot, []byte("lastSeq"))
	if err != nil {
		if err != ErrStateEntryNotFound {
			return err
		}

		lseq = 0

	}

	ss.snapshotLastSeq = lseq

	return nil
}

func (ss *StoreState) loadRev(s *Store) error {

	r, err := s.GetStateUint64(StateClassStore, StateGroupEvent, []byte("rev"))
	if err != nil {
		if err != ErrStateEntryNotFound {
			return err
		}

		r = 0
	}

	ss.rev.SetCount(r)

	return nil
}

func (ss *StoreState) loadLastSeq(s *Store) error {

	els, err := s.GetStateUint64(StateClassStore, StateGroupEvent, []byte("lastSeq"))
	if err != nil {
		if err != ErrStateEntryNotFound {
			return err
		}

		els = 0
	}

	ss.lastSeq.SetCount(els)

	return nil
}

func (ss *StoreState) loadCount(s *Store) error {

	ec, err := s.GetStateUint64(StateClassStore, StateGroupEvent, []byte("count"))
	if err != nil {
		if err != ErrStateEntryNotFound {
			return err
		}

		ec = 0
	}

	ss.count.SetCount(ec)

	return nil
}

func (ss *StoreState) syncSnapshotCount(s *Store, b *pebble.Batch) error {
	return s.SetStateUint64ByPath(b, StatePathSnapshotCount, ss.snapshotCount.Count())
}

func (ss *StoreState) syncSnapshotLastSeq(s *Store, b *pebble.Batch) error {
	return s.SetStateUint64ByPath(b, StatePathSnapshotLastSeq, ss.snapshotLastSeq)
}

func (ss *StoreState) syncRev(s *Store, b *pebble.Batch) error {
	return s.SetStateUint64ByPath(b, StatePathEventRev, ss.rev.Count())
}

func (ss *StoreState) syncLastSeq(s *Store, b *pebble.Batch) error {
	return s.SetStateUint64ByPath(b, StatePathEventLastSeq, ss.lastSeq.Count())
}

func (ss *StoreState) syncCount(s *Store, b *pebble.Batch) error {
	return s.SetStateUint64ByPath(b, StatePathEventCount, ss.count.Count())
}

type StoreOpt func(s *Store)

type Store struct {
	state      *StoreState
	eventstore *EventStore
	options    *Options
	name       string
	dbPath     string
	db         *pebble.DB
	batchPool  sync.Pool

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
		state:           &StoreState{},
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
	store.batchPool = sync.Pool{
		New: func() interface{} {
			return store.db.NewBatch()
		},
	}

	// Initializing scheduler
	store.timer = time.NewTimer(store.maxSyncInterval)
	store.timer.Stop()

	go store.sync()

	// Initializing column families
	store.cfState = NewColumnFamily(store, "state", PrefixState)
	store.cfEvent = NewColumnFamily(store, "event", PrefixEvent)
	store.cfSnapshot = NewColumnFamily(store, "snapshot", PrefixSnapshotData)

	// Loading states
	err = store.loadStates()
	if err != nil {
		return err
	}

	return nil
}

func (store *Store) loadStates() error {

	err := store.state.loadRev(store)
	if err != nil {
		return err
	}

	err = store.state.loadLastSeq(store)
	if err != nil {
		return err
	}

	err = store.state.loadCount(store)
	if err != nil {
		return err
	}

	err = store.state.loadSnapshotCount(store)
	if err != nil {
		return err
	}

	err = store.state.loadSnapshotLastSeq(store)
	if err != nil {
		return err
	}

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

func (store *Store) createNewSeq() uint64 {
	return store.state.lastSeq.Increase(1)
}

func (store *Store) State() *StoreState {
	return store.state
}

func (store *Store) Get(seq uint64) ([]byte, error) {

	// Preparing key-value
	key := Uint64ToBytes(seq)

	// Write
	value, closer, err := store.cfEvent.Get(nil, key)
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

	b := store.batchPool.Get().(*pebble.Batch)
	defer store.batchPool.Put(b)
	b.Reset()

	// Write
	err := store.cfEvent.Delete(b, key)
	if err != nil {
		return err
	}

	store.state.count.Increase(^uint64(0))

	// Update state to persistent store
	err = store.state.syncCount(store, b)
	if err != nil {
		return err
	}

	err = b.Commit(pebble.NoSync)
	if err != nil {
		return err
	}

	store.requestSync()

	return nil
}

func (store *Store) Write(data []byte, rev uint64) (uint64, error) {

	if rev > 0 {
		if store.state.Rev() >= rev {
			return 0, nil
		}
	}

	// Getting sequence
	seq := store.createNewSeq()

	// Preparing key-value
	key := Uint64ToBytes(seq)

	b := store.batchPool.Get().(*pebble.Batch)
	defer store.batchPool.Put(b)
	b.Reset()

	// Write
	err := store.cfEvent.Write(b, key, data)
	if err != nil {
		return 0, err
	}

	if rev > 0 {
		// update rev to persistent store
		store.state.rev.SetCount(rev)
		err = store.state.syncRev(store, b)
		if err != nil {
			return 0, err
		}
	}

	// sync seq to persistent store
	err = store.state.syncLastSeq(store, b)
	if err != nil {
		return 0, err
	}

	// update events count to persistent store
	store.state.count.Increase(1)
	err = store.state.syncCount(store, b)
	if err != nil {
		return 0, err
	}

	err = b.Commit(pebble.NoSync)
	if err != nil {
		return 0, err
	}

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

func (store *Store) GetDurableState(durableName string) (uint64, error) {
	return store.GetStateUint64([]byte("durable"), StrToBytes(durableName), []byte("lastSeq"))
}

func (store *Store) UpdateDurableState(b *pebble.Batch, durableName string, lastSeq uint64) error {
	return store.SetStateUint64(b, []byte("durable"), StrToBytes(durableName), []byte("lastSeq"), lastSeq)
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

	//if startAt+offset >= store.counter.Count() {
	if startAt+offset > store.state.LastSeq() {
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
