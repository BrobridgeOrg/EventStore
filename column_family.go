package eventstore

import (
	"bytes"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
)

type ListOptions struct {
	Prefix           []byte
	WithoutRawPrefix bool
}

type ColumnFamily struct {
	Store *Store
	Db    *pebble.DB
	Name  string

	merge  func([]byte, []byte) []byte
	closed chan struct{}

	isScheduled uint32
	timer       *time.Timer
}

func NewColumnFamily(store *Store, name string) *ColumnFamily {
	cf := &ColumnFamily{
		Store:       store,
		Name:        name,
		closed:      make(chan struct{}),
		isScheduled: 0,
		timer:       time.NewTimer(time.Second * 10),
		merge: func(oldValue []byte, newValue []byte) []byte {
			return newValue
		},
	}

	cf.timer.Stop()

	return cf
}

func (cf *ColumnFamily) sync() {

	cf.timer.Reset(time.Second * 10)

	for {

		select {
		case <-cf.timer.C:
			cf.Db.LogData(nil, pebble.Sync)

			cf.timer.Stop()
			cf.timer.Reset(time.Second * 10)

			atomic.StoreUint32(&cf.isScheduled, 0)
		case <-cf.closed:
			cf.timer.Stop()
			close(cf.closed)
			return
		}
	}
}

func (cf *ColumnFamily) requestSync() {

	if atomic.LoadUint32(&cf.isScheduled) != 0 {
		return
	}

	atomic.StoreUint32(&cf.isScheduled, 1)

	cf.timer.Stop()
	cf.timer.Reset(time.Millisecond * 100)
}

func (cf *ColumnFamily) Open() error {

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

	dbPath := filepath.Join(cf.Store.dbPath, cf.Name)
	db, err := pebble.Open(dbPath, opts)
	if err != nil {
		return err
	}

	cf.Db = db

	go cf.sync()

	return nil
}

func (cf *ColumnFamily) Close() error {
	cf.closed <- struct{}{}
	cf.Db.LogData(nil, pebble.Sync)
	return cf.Db.Close()
}

func (cf *ColumnFamily) Delete(key []byte) error {

	err := cf.Db.Delete(key, pebble.NoSync)
	if err != nil {
		return err
	}

	cf.requestSync()

	return nil
}

func (cf *ColumnFamily) Write(key []byte, data []byte) error {

	err := cf.Db.Set(key, data, pebble.NoSync)
	if err != nil {
		return err
	}

	cf.requestSync()

	return nil
}

func (cf *ColumnFamily) List(rawPrefix []byte, targetPrimaryKey []byte, opts *ListOptions) (*Cursor, error) {

	iterOpts := &pebble.IterOptions{}

	prefix := []byte("")
	if opts != nil && len(opts.Prefix) > 0 {

		prefix = opts.Prefix

		// Configuring upper bound
		upperBound := make([]byte, len(opts.Prefix))
		copy(upperBound, opts.Prefix)
		upperBound[len(upperBound)-1] = byte(int(upperBound[len(upperBound)-1]) + 1)

		fullUpperBound := bytes.Join([][]byte{
			rawPrefix,
			upperBound,
		}, []byte(""))

		iterOpts.UpperBound = fullUpperBound
	} else if len(rawPrefix) > 0 {

		// Configuring upper bound
		upperBound := make([]byte, len(rawPrefix))
		copy(upperBound, rawPrefix)
		upperBound[len(upperBound)-1] = byte(int(upperBound[len(upperBound)-1]) + 1)

		fullUpperBound := bytes.Join([][]byte{
			upperBound,
		}, []byte(""))

		iterOpts.UpperBound = fullUpperBound
	}

	targetKey := bytes.Join([][]byte{
		rawPrefix,
		prefix,
		targetPrimaryKey,
	}, []byte(""))

	iter := cf.Db.NewIter(iterOpts)

	iter.SeekGE(targetKey)

	cur := &Cursor{
		prefix: rawPrefix,
		isRaw:  !opts.WithoutRawPrefix,
		iter:   iter,
	}

	return cur, nil
}
